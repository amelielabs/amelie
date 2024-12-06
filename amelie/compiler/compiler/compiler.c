
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

void
compiler_init(Compiler*    self,
              Db*          db,
              Local*       local,
              FunctionMgr* function_mgr)
{
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	self->db       = db;
	self->code     = &self->code_node;
	self->args     = NULL;
	code_init(&self->code_coordinator);
	code_init(&self->code_node);
	code_data_init(&self->code_data);
	set_cache_init(&self->values_cache);
	parser_init(&self->parser, db, local, function_mgr, &self->code_data,
	            &self->values_cache);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	code_free(&self->code_coordinator);
	code_free(&self->code_node);
	code_data_free(&self->code_data);
	set_cache_free(&self->values_cache);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code     = &self->code_node;
	self->args     = NULL;
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	code_reset(&self->code_coordinator);
	code_reset(&self->code_node);
	code_data_reset(&self->code_data);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
}

void
compiler_parse_import(Compiler*    self, Str* text, Str* uri,
                      EndpointType type)
{
	parse_import(&self->parser, text, uri, type);
}

static void
emit_stmt(Compiler* self)
{
	// generate node code (pushdown)
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);

		// validate returning targets
		target_list_validate_dml(&stmt->target_list, insert->target);

		if (insert->on_conflict == ON_CONFLICT_NONE)
			emit_insert(self, stmt->ast);
		else
			emit_upsert(self, stmt->ast);
		break;
	}

	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);

		// validate returning targets
		// validate supported targets as expression or shared table
		target_list_validate_dml(&stmt->target_list, update->target);

		emit_update(self, stmt->ast);
		break;
	}

	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);

		// validate returning targets
		// validate supported targets as expression or shared table
		target_list_validate_dml(&stmt->target_list, delete->target);

		emit_delete(self, stmt->ast);
		break;
	}

	case STMT_SELECT:
	{
		// no targets or all targets are expressions
		if (target_list_is_expr(&stmt->target_list))
		{
			// select expr
			// select from [expr]
			// select (select expr)
			// select (select from [expr])
			// select (select expr) from [expr]
			// select (select from [expr]) from [expr]
			//
			// do not nothing (coordinator only)
			return;
		}

		// direct query from distributed or shared table
		auto select = ast_select_of(stmt->ast);
		if (select->target && select->target->from_table)
		{
			// select from table

			// validate supported targets as expressions or shared table
			target_list_validate(&stmt->target_list, select->target);

			// distributed table or shared table
			pushdown(self, stmt->ast);
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select (select from table)
			if (!select->target || !select->target->from_select)
				error("FROM: undefined distributed table");

			// select from (select from table)
			auto from_select = ast_select_of(select->target->from_select);
			if (from_select->target == NULL ||
			    from_select->target->from_table == NULL)
				error("FROM SELECT: undefined distributed table");

			// validate FROM SELECT targets
			target_list_validate(&stmt->target_list, from_select->target);

			// generate pushdown as a nested query
			pushdown(self, &from_select->ast);
			break;
		}

		// expression or shared table targets only

		// select (select from shared)
		// select expr(select from shared)
	
		// select pushdown to the first node
		pushdown_first(self, stmt->ast);
		break;
	}

	case STMT_WATCH:
	{
		// do nothing (coordinator only)
		if (! target_list_is_expr(&stmt->target_list))
			error("WATCH: sub queries are not supported");
		return;
	}

	default:
		abort();
		break;
	}

	// CRET
	op0(self, CRET);
}

static inline void
emit_send_generated_on_match(Compiler* self, void* arg)
{
	AstInsert* insert = arg;
	// generate and push to the stack each generated
	// column expression
	auto op = insert->generated_columns;
	for (; op; op = op->next)
	{
		auto column = op->l->column;

		// expr
		int rexpr = emit_expr(self, insert->target_generated, op->r);
		int type = rtype(self, rexpr);

		// ensure that the expression type is compatible
		// with the column
		if (unlikely(type != TYPE_NULL && column->type != type))
			error("<%.*s.%.*s> column generated expression type '%s' does not match column type '%s'",
			      str_size(&insert->target->name), str_of(&insert->target->name),
			      str_size(&column->name), str_of(&column->name),
			      type_of(type),
			      type_of(column->type));

		// push
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}
}

static inline void
emit_send_generated(Compiler* self, int start)
{
	auto stmt   = self->current;
	auto insert = ast_insert_of(stmt->ast);
	auto table  = insert->target->from_table;

	// store_open( insert->values )
	auto target = insert->target_generated;
	target->r = op2(self, CSET_PTR, rpin(self, TYPE_STORE),
	                (intptr_t)insert->values);

	// generate scan over insert rows to create new rows using the
	// generated columns expressions
	scan(self, target,
	     NULL,
	     NULL,
	     NULL,
	     emit_send_generated_on_match,
	     insert);

	// CSEND_GENERATED
	op4(self, CSEND_GENERATED, stmt->order, start,
	    (intptr_t)table,
	    (intptr_t)insert->values);
}

static inline void
emit_send(Compiler* self, int start)
{
	// generate coordinator send code
	Target* target = NULL;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		auto table = insert->target->from_table;
		if (table_columns(table)->count_stored > 0) {
			// CSEND_GENERATED
			emit_send_generated(self, start);
		} else {
			// CSEND
			op4(self, CSEND, stmt->order, start,
			    (intptr_t)table,
			    (intptr_t)insert->values);
		}
		break;
	}
	case STMT_UPDATE:
		target = ast_update_of(stmt->ast)->target;
		break;
	case STMT_DELETE:
		target = ast_delete_of(stmt->ast)->target;
		break;
	case STMT_SELECT:
	{
		// no targets or all targets are expressions
		if (target_list_is_expr(&stmt->target_list))
			break;

		// direct query from distributed or shared table
		auto select = ast_select_of(stmt->ast);
		if (select->target && select->target->from_table)
		{
			target = select->target;
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select from (select from table)
			target = ast_select_of(select->target->from_select)->target;
			break;
		}

		// nested expression or nested shared table targets

		// CSEND_FIRST
		op2(self, CSEND_FIRST, stmt->order, start);
		break;
	}

	case STMT_WATCH:
		// no targets
		break;

	default:
		abort();
		break;
	}

	// table target
	if (! target)
		return;

	auto table = target->from_table;
	if (table->config->shared)
	{
		// send to first node

		// CSEND_FIRST
		op2(self, CSEND_FIRST, stmt->order, start);
	} else
	{
		// point-lookup or range scan
		if (ast_path_of(target->path)->type == PATH_LOOKUP)
		{
			// send to one node (shard by lookup key hash)
			uint32_t hash = target_lookup_hash(target);

			// CSEND_HASH
			op4(self, CSEND_HASH, stmt->order, start, (intptr_t)table, hash);
		} else
		{
			// send to all table nodes

			// CSEND_ALL
			op3(self, CSEND_ALL, stmt->order, start, (intptr_t)table);
		}
	}
}

static inline void
emit_recv(Compiler* self)
{
	Returning* ret = NULL;
	int r = -1;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		ret = &insert->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_UPDATE:
	{
		auto update = ast_update_of(stmt->ast);
		ret = &update->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);
		ret = &delete->ret;
		r = pushdown_recv_returning(self, returning_has(ret));
		break;
	}

	case STMT_SELECT:
	{
		auto select = ast_select_of(stmt->ast);
		ret = &select->ret;

		// no targets or all targets are expressions
		if (target_list_is_expr(&stmt->target_list))
		{
			r = emit_select(self, stmt->ast);
			break;
		}

		// direct query from distributed or shared table
		if (select->target && select->target->from_table)
		{
			r = pushdown_recv(self, stmt->ast);
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select over returned SET or MERGE object
			select->target->r = pushdown_recv(self, select->target->from_select);
			r = emit_select(self, stmt->ast);
			break;
		}

		// nested expression or nested shared table targets
		// (first node only, single result)

		// CRECV_TO (note: set or merge received)
		r = op2(self, CRECV_TO, rpin(self, TYPE_STORE), stmt->order);
		break;
	}

	case STMT_WATCH:
		// no targets (coordinator only)
		r = emit_watch(self, stmt->ast);
		break;

	default:
		abort();
		break;
	}

	auto has_result = r != -1;
	if (! has_result)
		r = op1(self, CNULL, rpin(self, TYPE_NULL));

	// CCTE_SET
	op2(self, CCTE_SET, stmt->cte->id, r);
	runpin(self, r);

	// statement returns
	if (stmt->ret)
	{
		// create content using cte result
		if (has_result)
			op3(self, CCONTENT, stmt->cte->id,
			    (intptr_t)&ret->columns,
			    (intptr_t)&ret->format);
		op0(self, CRET);
	}
}

static inline void
emit_recv_upto(Compiler* self, int last, int order)
{
	auto current = self->current;
	list_foreach(&self->parser.stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		if (stmt->order <= last)
			continue;
		if (stmt->order > order)
			break;
		// <= recv
		self->current = stmt;	
		emit_recv(self);
	}
	self->current = current;
}

hot void
compiler_emit(Compiler* self)
{
	auto parser = &self->parser;
	auto stmt_list = &parser->stmt_list;
	assert(stmt_list->list_count > 0);
	rmap_prepare(&self->map);

	// analyze statements
	//
	// find last statement which access a table, mark as required
	// snapshot if there are two or more statements which access the table
	int count = 0;
	self->last = analyze_stmts(parser, &count);
	if (count >= 2)
		self->snapshot = true;

	// same applies to shared tables DML
	if (parser_is_shared_table_dml(parser))
		self->snapshot = true;

	// process statements
	auto recv_last = -1;
	list_foreach(&stmt_list->list)
	{
		auto stmt = list_at(Stmt, link);

		// generate node code (pushdown)
		auto stmt_start = code_count(&self->code_node);
		compiler_switch_node(self);
		self->current = stmt;
		emit_stmt(self);

		// generate coordinator code
		compiler_switch_coordinator(self);

		// generate recv up to the max dependable statement order, including
		// coordinator only expressions
		auto stmt_is_expr = target_list_is_expr(&stmt->target_list);
		auto recv = -1;
		if (stmt_is_expr)
			recv = stmt->order;
		else
			recv = cte_deps_max_stmt(&stmt->cte_deps);
		if (recv >= 0)
		{
			emit_recv_upto(self, recv_last, recv);
			recv_last = recv;
		}

		// RETURN stmt
		if (stmt->ret)
		{
			if (! stmt_is_expr)
				emit_send(self, stmt_start);
			emit_recv_upto(self, recv_last, stmt->order);
			recv_last = stmt->order;
			continue;
		}

		// skip expression only statements (generated by recv above)
		if (stmt_is_expr)
			continue;

		// generate SEND command
		emit_send(self, stmt_start);
	}

	// recv rest of commands (if any left)
	emit_recv_upto(self, recv_last, stmt_list->list_count);

	// no statements (last statement always returns)
	if (! parser->stmt)
		op0(self, CRET);
}

void
compiler_program(Compiler* self, Program* program)
{
	program->code       = &self->code_coordinator;
	program->code_node  = &self->code_node;
	program->code_data  = &self->code_data;
	program->stmts      = self->parser.stmt_list.list_count;
	program->stmts_last = -1;
	program->ctes       = self->parser.cte_list.list_count;
	program->snapshot   = self->snapshot;
	program->repl       = false;
	if (self->last)
		program->stmts_last = self->last->order;
}
