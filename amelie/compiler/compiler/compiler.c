
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>

void
compiler_init(Compiler* self, Local* local)
{
	self->program   = NULL;
	self->code      = NULL;
	self->code_data = NULL;
	self->sends     = 0;
	self->args      = NULL;
	self->current   = NULL;
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, local, &self->values_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->program   = NULL;
	self->code      = NULL;
	self->code_data = NULL;
	self->sends     = 0;
	self->args      = NULL;
	self->current   = NULL;
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_set(Compiler* self, Program* program)
{
	self->program   = program;
	self->code      = &self->program->code;
	self->code_data = &self->program->code_data;
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, self->program, text);
}

void
compiler_parse_import(Compiler*    self, Str* text, Str* uri,
                      EndpointType type)
{
	parse_import(&self->parser, self->program, text, uri, type);
}

static void
emit_send(Compiler* self, Target* target, int start)
{
	auto stmt = self->current;
	auto ret  = stmt->ret;

	// prepare union to use for the distributed operation result
	auto rret = -1;
	if (ret && returning_has(ret))
	{
		rret   = op1(self, CUNION, rpin(self, TYPE_STORE));
		ret->r = rret;
	}

	// table target pushdown
	auto table = target->from_table;

	// set snapshot if two or more stmts are sending data
	// and using tables
	auto program = self->program;
	if (self->sends > 0)
		program->snapshot = true;
	self->sends++;

	// INSERT
	if (stmt->id == STMT_INSERT)
	{
		// create or match a set to use with the insert operation
		auto r = emit_insert_store(self);

		// CSEND_SHARD
		op4(self, CSEND_SHARD, start, (intptr_t)table, r, rret);
		runpin(self, r);
	} else
	{
		// UPDATE, DELETE, SELECT
		//
		// point-lookup or range scan
		//
		auto path = target->path_primary;
		if (path->type == PATH_LOOKUP && !path->match_start_columns)
		{
			if (! path->match_start_vars)
			{
				// match exact partition using the point lookup const key hash
				uint32_t hash = path_create_hash(path);

				// CSEND_LOOKUP
				op4(self, CSEND_LOOKUP, start, (intptr_t)table, hash, rret);
			} else
			{
				// match exact partition using the point lookup exprs
				for (auto i = 0; i < path->match_start; i++)
				{
					auto value = path->keys[i].start;
					auto rexpr = emit_expr(self, target->targets, value);
					op1(self, CPUSH, rexpr);
					runpin(self, rexpr);
				}

				// CSEND_LOOKUP_BY
				op3(self, CSEND_LOOKUP_BY, start, (intptr_t)table, rret);
			}
		} else
		{
			// send to all table partitions (one or more)

			// CSEND_ALL
			op3(self, CSEND_ALL, start, (intptr_t)table, rret);
		}
	}

	// mark last sending operation in the main block
	auto block = compiler_block(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		program->send_last = op_pos(self) - 1;
}

static void
emit_recv(Compiler* self, Stmt* stmt)
{
	// Select or DML Returning with target
	auto ret = stmt->ret;
	if (!ret || ret->r == -1 || ret->r_recv)
		return;

	if (stmt->r != -1)
		return;

	if (stmt->id == STMT_SELECT)
	{
		auto select = ast_select_of(stmt->ast);
		if (! select->pushdown)
			return;
	}

	// CUNION_RECV
	//
	// receive results and add them to the union
	//
	op1(self, CUNION_RECV, ret->r);
	ret->r_recv = true;

	// process pushdown result
	if (stmt->id == STMT_SELECT) {
		auto stmt_prev = self->current;
		self->current = stmt;
		stmt->r = pushdown_recv(self, stmt->ast);
		self->current = stmt_prev;
	} else {
		stmt->r = ret->r;
	}
}

hot static void
emit_sync(Compiler* self, Stmt* stmt)
{
	compiler_switch_frontend(self);
	if (stmt->id == STMT_INSERT)
	{
		auto insert = ast_insert_of(stmt->ast);
		if (insert->select)
			emit_recv(self, insert->select);
		return;
	}
	for (auto ref = stmt->select_list.list; ref; ref = ref->next)
	{
		auto select = ast_select_of(ref->ast);
		for (auto target = select->targets.list; target; target = target->next)
		{
			if (target->type != TARGET_STMT)
				continue;
			emit_recv(self, target->from_stmt);
		}
	}
}

hot static int
emit_stmt_backend(Compiler* self, Stmt* stmt)
{
	// generate backend code (pushdown)
	compiler_switch_backend(self);
	auto start = code_count(&self->program->code_backend);

	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		if (insert->on_conflict == ON_CONFLICT_NONE)
			emit_insert(self, stmt->ast);
		else
			emit_upsert(self, stmt->ast);
		break;
	}
	case STMT_UPDATE:
	{
		emit_update(self, stmt->ast);
		break;
	}
	case STMT_DELETE:
	{
		emit_delete(self, stmt->ast);
		break;
	}
	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		if (select->pushdown)
		{
			// table direct scan or join
			//
			// execute on one or more backends, process the result on frontend
			//
			pushdown(self, stmt->ast);
			break;
		}

		// select (select from table)
		// select expr
		//
		// do nothing (frontend only)
		return -1;
	}
	case STMT_WATCH:
		// do nothing (frontend only)
		return -1;
	default:
		abort();
		break;
	}

	// CRET
	op0(self, CRET);
	return start;
}

hot static void
emit_assign(Compiler* self, Stmt* stmt)
{
	// assign :=
	auto ret = stmt->ret;
	auto var = stmt->assign;

	// generate recv if stmt result is expected for := or return
	emit_recv(self, stmt);
	if (! ret)
		stmt_error(stmt, NULL, "statement cannot be assigned");
	assert(stmt->r != -1);

	if (var->type == TYPE_STORE)
	{
		// compare columns
		auto type = rtype(self, stmt->r);
		if (type != TYPE_NULL && !columns_compare(&var->columns, &ret->columns))
			stmt_error(self->current, stmt->ast, "variable table columns mismatch");

		// var = store
		op2(self, CASSIGN_STORE, var->r, stmt->r);
	} else
	{
		if (ret->count > 1)
			stmt_error(stmt, NULL, "statement must return only one column to be assigned");

		auto type = columns_first(&ret->columns)->type;
		if (type != TYPE_NULL && var->type != type)
			stmt_error(self->current, stmt->ast, "variable expected %s",
			           type_of(var->type));

		op2(self, CASSIGN, var->r, stmt->r);
	}

	runpin(self, stmt->r);
	stmt->r = -1;
}

hot static void
emit_stmt(Compiler* self, Stmt* stmt)
{
	auto stmt_prev = self->current;
	self->current = stmt;

	// generate all dependable recv statements first
	emit_sync(self, stmt);

	// generate backend code (pushdown)
	auto start = emit_stmt_backend(self, stmt);

	// generate frontend code
	compiler_switch_frontend(self);
	Target* target = NULL;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		target = targets_outer(&insert->targets);
		break;
	}
	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		target = targets_outer(&update->targets);
		break;
	}
	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		target = targets_outer(&delete->targets);
		break;
	}
	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		target = select->pushdown;

		// table scan or join
		//
		// execute on one or more backends, process the result on frontend
		//
		if (select->pushdown)
			break;

		// select (select from table)
		// select expr
		//
		// emit expression for return
		//
		stmt->r = emit_select(self, stmt->ast, false);
		break;
	}
	case STMT_WATCH:
		// no targets
		emit_watch(self, stmt->ast);
		break;
	default:
		abort();
		break;
	}

	// set cte columns types
	if (stmt->cte_name)
		columns_copy_types(&stmt->cte_columns, &stmt->ret->columns);

	// generate frontend send command based on the target
	if (target)
		emit_send(self, target, start);

	// assign :=
	if (stmt->assign)
		emit_assign(self, stmt);

	// set previous stmt
	self->current = stmt_prev;
}

hot static void
emit_block(Compiler*, Block*);

hot static void
emit_if(Compiler* self, Stmt* stmt)
{
	auto stmt_prev = self->current;
	self->current = stmt;

	auto ifa = ast_if_of(stmt->ast);
	compiler_switch_frontend(self);

	// jmp to start
	auto _start_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _stop_jmp
	auto _stop_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _stop */);

	// _start
	auto _start = op_pos(self);
	op_set_jmp(self, _start_jmp, _start);

	// foreach cond
	//   stmt
	//   jntr _next
	//   block
	//   jmp _stop
	//
	// else_block
	// _stop
	//
	auto ref = ifa->conds.list;
	for (; ref; ref = ref->next)
	{
		auto cond = ast_if_cond_of(ref->ast);

		// IF expr
		auto r = emit_expr(self, &ifa->targets, cond->expr);

		// jntr _next
		int _next_jntr = op_pos(self);
		op2(self, CJNTR, 0 /* _next */, r);
		runpin(self, r);

		// THEN block

		// block
		emit_block(self, cond->block);

		op1(self, CJMP, _stop_jmp);

		// _next
		op_set_jmp(self, _next_jntr, op_pos(self));
		op_at(self, _next_jntr)->a = op_pos(self);
	}

	// else block
	if (ifa->cond_else)
		emit_block(self, ifa->cond_else);

	// _stop
	auto _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);

	// mark last sending operation in the main block
	auto block = compiler_block(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		op0(self, CCLOSE);

	// set previous stmt
	self->current = stmt_prev;
}

static void
emit_for_on_match(Compiler* self, Targets* targets, void* arg)
{
	unused(targets);
	AstFor* fora = arg;
	emit_block(self, fora->block);
}

hot static void
emit_for(Compiler* self, Stmt* stmt)
{
	auto stmt_prev = self->current;
	self->current = stmt;

	auto fora = ast_for_of(stmt->ast);
	compiler_switch_frontend(self);

	// generate all dependable recv statements first
	auto target = targets_outer(&fora->targets);
	assert(target);
	if (target->type == TARGET_STMT)
		emit_recv(self, target->from_stmt);

	// scan over targets
	scan(self,
	     &fora->targets,
	     NULL,
	     NULL,
	     NULL,
	     emit_for_on_match,
	     fora);

	// mark last sending operation in the main block
	auto block = compiler_block(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		op0(self, CCLOSE);

	// set previous stmt
	self->current = stmt_prev;
}

hot static void
emit_block(Compiler* self, Block* block)
{
	auto stmt_prev = self->current;
	self->current = NULL;

	// reserve variables
	auto var = block->vars.list;
	for (; var; var = var->next)
		var->r = rpin(self, var->type);

	// emit statements in the block, track last statement
	Stmt* last = NULL;
	auto stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		if (stmt->id == STMT_IF)
			emit_if(self, stmt);
		else
		if (stmt->id == STMT_FOR)
			emit_for(self, stmt);
		else
			emit_stmt(self, stmt);
		last = stmt;
	}

	// ensure all pending returning statements are received
	// on block exit
	stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
		emit_recv(self, stmt);

	// create result content for last stmt of the main block
	if (block == compiler_block(self))
	{
		if (last && last->r != -1)
			op3(self, CCONTENT, last->r,
			    (intptr_t)&last->ret->columns,
			    (intptr_t)&last->ret->format);

		op0(self, CRET);
	} else
	{
		// free variable on block exit
		var = block->vars.list;
		for (; var; var = var->next)
		{
			op1(self, CFREE, var->r);
			runpin(self, var->r);
			var->r = -1;
		}
	}

	// set previous stmt
	self->current = stmt_prev;
}

hot void
compiler_emit(Compiler* self)
{
	// ddl/utility or dml/query
	auto main = self->parser.blocks.list;
	assert(main);

	if (stmt_is_utility(compiler_stmt(self)))
		emit_utility(self);
	else
		emit_block(self, main);

	// set the max number of registers used
	code_set_regs(&self->program->code, self->map.count);
	code_set_regs(&self->program->code_backend, self->map.count);
}
