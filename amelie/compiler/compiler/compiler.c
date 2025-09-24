
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
	self->args      = NULL;
	self->current   = NULL;
	self->last      = NULL;
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
	self->args      = NULL;
	self->current   = NULL;
	self->last      = NULL;
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
emit_send(Compiler* self, Target* target, Returning* ret, int start)
{
	auto stmt = self->current;

	// prepare union to use for the distributed operation result
	if (returning_has(ret))
		ret->r = op1(self, CUNION, rpin(self, TYPE_STORE));

	// table target pushdown
	auto table = target->from_table;

	// INSERT
	if (stmt->id == STMT_INSERT)
	{
		// create or match a set to use with the insert operation
		auto r = emit_insert_store(self);

		// CSEND_SHARD
		self->program->send_last = op_pos(self);
		op4(self, CSEND_SHARD, start, (intptr_t)table, r, ret->r);
		runpin(self, r);
		return;
	}

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
			self->program->send_last = op_pos(self);
			op4(self, CSEND_LOOKUP, start, (intptr_t)table, hash, ret->r);
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
			self->program->send_last = op_pos(self);
			op3(self, CSEND_LOOKUP_BY, start, (intptr_t)table, ret->r);
		}
		return;
	}

	// send to all table partitions (one or more)

	// CSEND_ALL
	self->program->send_last = op_pos(self);
	op3(self, CSEND_ALL, start, (intptr_t)table, ret->r);
}

static void
emit_recv(Compiler* self, Stmt* stmt)
{
	if (stmt->r != -1)
		return;

	Returning* ret = NULL;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		ret = &insert->ret;
		break;
	}
	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		ret = &update->ret;
		break;
	}
	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		ret = &delete->ret;
		break;
	}
	case STMT_SELECT:
	{
		auto select = ast_select_of(stmt->ast);
		ret = &select->ret;
		if (! select->pushdown)
			return;
		break;
	}
	case STMT_WATCH:
		return;
	default:
		abort();
		break;
	}
	if (ret->r == -1 || ret->r_recv)
		return;

	// CUNION_RECV
	//
	// receive results and add them to the union
	//
	op1(self, CUNION_RECV, ret->r);
	ret->r_recv = true;

	// process pushdown result
	if (stmt->id == STMT_SELECT)
		stmt->r = pushdown_recv(self, stmt->ast);
	else
		stmt->r = ret->r;
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
			if (target->type != TARGET_CTE)
				continue;
			emit_recv(self, target->from_cte);
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
emit_stmt(Compiler* self, Stmt* stmt)
{
	self->current = stmt;

	// generate all dependable recv statements first
	emit_sync(self, stmt);

	// generate backend code (pushdown)
	auto start = emit_stmt_backend(self, stmt);

	// generate frontend code
	compiler_switch_frontend(self);
	Returning* ret = NULL;
	Target* target = NULL;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		ret = &insert->ret;
		target = targets_outer(&insert->targets);
		break;
	}
	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		ret = &update->ret;
		target = targets_outer(&update->targets);
		break;
	}
	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		ret = &delete->ret;
		target = targets_outer(&delete->targets);
		break;
	}
	case STMT_SELECT:
	{
		// select from table, ...
		auto select = ast_select_of(stmt->ast);
		ret = &select->ret;
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
		stmt->r = emit_select(self, stmt->ast);
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

	// generate frontend send command based on the target
	if (target)
	{
		emit_send(self, target, ret, start);

		// generate recv if stmt result is expected for := or return
		if (stmt->assign || stmt->ret)
			emit_recv(self, stmt);
	}

	// assign :=
	auto var = stmt->assign;
	if (var)
	{
		if (! ret)
			stmt_error(stmt, NULL, "statement cannot be assigned");
		if (ret->count > 1)
			stmt_error(stmt, NULL, "statement must return only one column to be assigned");
		// argument assignment
		auto type = columns_first(&ret->columns)->type;
		if (var->type != TYPE_NULL && var->type != type)
			stmt_error(self->current, stmt->ast, "variable expected %s",
			           type_of(var->type));
		assert(stmt->r != -1);
		var->type = type;
		var->r = op2(self, CASSIGN, rpin(self, var->type), stmt->r);
	}

	// return
	if (stmt->ret)
	{
		// ensure all pending statements being received
		auto ref = stmt->block->stmts.list;
		for (; ref; ref = ref->next)
			emit_recv(self, ref);

		// create result content
		if (stmt->r != -1)
			op3(self, CCONTENT, stmt->r,
			    (intptr_t)&ret->columns,
			    (intptr_t)&ret->format);
		op0(self, CRET);
	}

	// set last statement which uses a table,
	// set snapshot if two or more stmts are using tables
	if (self->last)
		self->program->snapshot = true;
	self->last = self->current;
}

hot static void
emit_block(Compiler* self, Block* block)
{
	auto stmt = block->stmts.list;
	for (; stmt; stmt = stmt->next)
		emit_stmt(self, stmt);
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
