
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
compiler_init(Compiler* self, Local* local, SetCache* set_cache)
{
	self->program   = NULL;
	self->set_cache = set_cache;
	self->current   = NULL;
	self->code      = NULL;
	self->code_data = NULL;
	self->origin    = ORIGIN_FRONTEND;
	self->sends     = 0;
	parser_init(&self->parser, local, self->set_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->program   = NULL;
	self->code      = NULL;
	self->code_data = NULL;
	self->sends     = 0;
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
compiler_parse_udf(Compiler* self, Udf* udf)
{
	parse_udf(&self->parser, self->program, udf);
}

void
compiler_parse_import(Compiler*    self, Str* text, Str* uri,
                      EndpointType type)
{
	parse_import(&self->parser, self->program, text, uri, type);
}

static void
emit_close(Compiler* self, Stmt* stmt)
{
	// mark last sending operation in the main block
	auto block = compiler_main(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		op0(self, CCLOSE);
}

static void
emit_send(Compiler* self, Target* target, int start)
{
	auto stmt = self->current;
	auto ret  = stmt->ret;

	// push references
	auto refs_count = stmt->refs.count;
	auto ref = stmt->refs.list;
	while (ref)
	{
		if (ref->ast) {
			auto r = emit_expr(self, ref->from, ref->ast);
			op3(self, CPUSH_REF, r, 0, ref->not_null);
			runpin(self, r);
		} else {
			op3(self, CPUSH_REF, ref->r, 1, ref->not_null);
		}
		ref = ref->next;
	}

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
		op5(self, CSEND_SHARD, start, (intptr_t)table, r, rret, refs_count);
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
				op5(self, CSEND_LOOKUP, start, (intptr_t)table, hash, rret, refs_count);
			} else
			{
				// match exact partition using the point lookup exprs
				for (auto i = 0; i < path->match_start; i++)
				{
					auto value = path->keys[i].start;
					auto rexpr = emit_expr(self, target->from, value);
					op1(self, CPUSH, rexpr);
					runpin(self, rexpr);
				}

				// CSEND_LOOKUP_BY
				op4(self, CSEND_LOOKUP_BY, start, (intptr_t)table, rret, refs_count);
			}
		} else
		{
			// send to all table partitions (one or more)

			// CSEND_ALL
			op4(self, CSEND_ALL, start, (intptr_t)table, rret, refs_count);
		}
	}

	// mark last sending operation in the main block
	auto block = compiler_main(self);
	if (stmt->block == block && block->stmts.last_send == stmt)
		program->send_last = op_pos(self) - 1;
}

hot static void
emit_into(Compiler* self, Stmt* stmt)
{
	// INTO var, ... and := operator
	auto ret = stmt->ret;
	if (! ret)
		stmt_error(stmt, NULL, "statement cannot be assigned");
	if (! ret->count_into)
		return;

	// statement must received at this point
	assert(stmt->r != -1);

	auto var = stmt->ret->list_into->var;
	if (var->type == TYPE_STORE)
	{
		// INTO table_var

		// ensure only one table var used
		if (ret->count_into > 1)
			stmt_error(self->current, stmt->ast, "INTO accept only one table variable");

		// compare columns
		auto type = rtype(self, stmt->r);
		if (type != TYPE_NULL && !columns_compare(&var->columns, &ret->columns))
			stmt_error(self->current, stmt->ast, "variable table columns mismatch");

		// CVAR_MOV (var = store)
		op3(self, CVAR_MOV, var->order, var->is_arg, stmt->r);

		runpin(self, stmt->r);
		stmt->r = -1;
		return;
	}

	// INTO var, ...
	auto ref = stmt->ret->list_into;
	list_foreach(&ret->columns.list)
	{
		auto column = list_at(Column, link);

		// all variables must be resolved at this point
		auto var  = ref->var;
		if (var->type == TYPE_STORE)
			stmt_error(self->current, stmt->ast, "INTO accept only one table variable");

		auto type = column->type;
		if (type != TYPE_NULL && var->type != type)
			stmt_error(self->current, stmt->ast, "variable expected %s",
					   type_of(var->type));

		// CVAR_SET
		op4(self, CVAR_SET, var->order, var->is_arg, stmt->r, column->order);

		// variables count can be less than expressions
		ref = ref->next;
		if (! ref)
			break;
	}

	// CFREE
	op1(self, CFREE, stmt->r);
	runpin(self, stmt->r);
	stmt->r = -1;
}

static void
emit_recv(Compiler* self, Stmt* stmt)
{
	// Select or DML Returning with target
	auto ret = stmt->ret;
	if (!ret || ret->r == -1 || ret->r_recv)
		return;

	// CUNION_RECV
	//
	// receive results and add them to the union
	//
	op1(self, CUNION_RECV, ret->r);
	ret->r_recv = true;

	auto stmt_prev = self->current;
	self->current = stmt;

	// process pushdown result
	if (stmt->id == STMT_SELECT) {
		stmt->r = pushdown_recv(self, stmt->ast);
	} else {
		stmt->r = ret->r;
	}

	// handle INTO and := after receive
	if (ret->count_into > 0)
		emit_into(self, stmt);

	self->current = stmt_prev;
}

hot static int
emit_stmt_backend(Compiler* self, Stmt* stmt)
{
	// generate backend code (pushdown)
	compiler_switch_backend(self);
	auto start = code_count(&self->program->code_backend);

	int r = -1;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		if (insert->on_conflict == ON_CONFLICT_NONE)
			emit_insert(self, stmt->ast);
		else
			r = emit_upsert(self, stmt->ast);
		break;
	}
	case STMT_UPDATE:
	{
		r = emit_update(self, stmt->ast);
		break;
	}
	case STMT_DELETE:
	{
		r = emit_delete(self, stmt->ast);
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
			r = pushdown(self, stmt->ast);
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
	op1(self, CRET, r);
	if (r != -1)
		runpin(self, r);
	return start;
}

hot static void
emit_stmt(Compiler* self, Stmt* stmt)
{
	auto stmt_prev = self->current;
	self->current = stmt;

	// generate backend code (pushdown)
	auto start = emit_stmt_backend(self, stmt);

	// generate frontend code
	compiler_switch_frontend(self);
	Target* target = NULL;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		target = from_first(&insert->from);
		break;
	}
	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		target = from_first(&update->from);
		break;
	}
	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		target = from_first(&delete->from);
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
	if (target) {
		emit_send(self, target, start);
	} else
	{
		// INTO and := (only for expressions)
		if (stmt->ret && stmt->ret->count_into > 0)
			emit_into(self, stmt);

		// emit close for last distributed SELECT udf()
		if (stmt->udfs_sending)
			emit_close(self, stmt);
	}

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
		auto r = emit_expr(self, &ifa->from, cond->expr);

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
	emit_close(self, stmt);

	// set previous stmt
	self->current = stmt_prev;
}

static void
emit_for_on_match(Compiler* self, From* from, void* arg)
{
	unused(from);
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

	// scan over from
	scan(self,
	     &fora->from,
	     NULL,
	     NULL,
	     NULL,
	     emit_for_on_match,
	     fora);

	// set snapshot if loop is using send command
	if (fora->block->stmts.last_send)
		self->program->snapshot = true;

	// mark last sending operation in the main block
	emit_close(self, stmt);

	// set previous stmt
	self->current = stmt_prev;
}

hot static void
emit_return(Compiler* self, Stmt* stmt)
{
	compiler_switch_frontend(self);

	auto r = -1;
	if (stmt->id == STMT_RETURN)
	{
		// null
	} else
	if (stmt->ret && stmt->ret->columns.count > 0)
	{
		emit_recv(self, stmt);
		r =  stmt->r;

		// validate return type
		auto udf = stmt->block->ns->udf;
		if (udf && udf->type != rtype(self, r))
			stmt_error(stmt, stmt->ast, "RETURN type '%s' mismatch function type '%s'",
			           type_of(rtype(self, r)),
			           type_of(udf->type));
	}
	op1(self, CRET, r);
}

hot static void
emit_block(Compiler* self, Block* block)
{
	auto stmt_prev = self->current;
	self->current = NULL;

	// emit statements in the block
	auto stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
	{
		// recv all dependable statements first
		compiler_switch_frontend(self);
		for (auto dep = stmt->deps.list; dep; dep = dep->next)
			emit_recv(self, dep->stmt);

		switch (stmt->id) {
		case STMT_IF:
			emit_if(self, stmt);
			break;
		case STMT_FOR:
			emit_for(self, stmt);
			break;
		case STMT_RETURN:
			break;
		case STMT_EXECUTE:
			// never handled here
			abort();
			break;
		default:
			emit_stmt(self, stmt);
			break;
		}

		// RETURN var | stmt
		if (stmt->is_return)
			emit_return(self, stmt);
	}

	// on block exit (not main)
	if (block != compiler_main(self))
	{
		// ensure all pending returning statements are received
		auto last = block->stmts.list_tail;
		if (last && !last->is_return)
		{
			stmt = block->stmts.list;
			for (; stmt; stmt = stmt->next)
				emit_recv(self, stmt);
		}
	}

	// set previous stmt
	self->current = stmt_prev;
}

hot void
compiler_emit(Compiler* self)
{
	// ddl/utility or dml/query
	auto main = compiler_namespace(self);
	assert(main);

	if (stmt_is_utility(compiler_stmt(self)))
	{
		emit_utility(self);
	} else
	{
		// initialize declared variables (skipping arguments)
		auto vars = 0;
		for (auto var = main->vars.list; var; var = var->next)
			if (! var->is_arg)
				vars++;
		if (vars > 0)
			op1(self, CPUSH_NULLS, vars);

		// emit main block
		emit_block(self, main->blocks.list);
	}

	// set the max number of registers used
	code_set_regs(&self->program->code, self->map.count);
	code_set_regs(&self->program->code_backend, self->map.count);
}
