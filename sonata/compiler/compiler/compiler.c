
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>

void
compiler_init(Compiler*    self,
              Db*          db,
              FunctionMgr* function_mgr)
{
	self->snapshot     = false;
	self->current      = NULL;
	self->last         = NULL;
	self->function_mgr = function_mgr;
	self->db           = db;
	self->code         = &self->code_shard;
	code_init(&self->code_coordinator);
	code_init(&self->code_shard);
	code_data_init(&self->code_data);
	parser_init(&self->parser, db, &self->code_data);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_free(&self->parser);
	code_free(&self->code_coordinator);
	code_free(&self->code_shard);
	code_data_free(&self->code_data);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code     = &self->code_shard;
	self->snapshot = false;
	self->current  = NULL;
	self->last     = NULL;
	code_reset(&self->code_coordinator);
	code_reset(&self->code_shard);
	code_data_reset(&self->code_data);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
}

static void
emit_stmt(Compiler* self)
{
	auto stmt = self->current;
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
		auto update = ast_update_of(stmt->ast);

		// validate join tables
		// validate supported targets as expression or reference table
		target_list_validate_dml(&stmt->target_list, update->target);

		emit_update(self, stmt->ast);
		break;
	}

	case STMT_DELETE:
	{
		auto delete = ast_delete_of(stmt->ast);

		// validate join tables
		// validate supported targets as expression or reference table
		target_list_validate_dml(&stmt->target_list, delete->target);

		emit_delete(self, stmt->ast);
		break;
	}

	case STMT_SELECT:
	{
		// no targets or all targets are expressions
		if (target_list_expr(&stmt->target_list))
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

		// direct query from distributed or reference table
		auto select = ast_select_of(stmt->ast);
		if (select->target && select->target->table)
		{
			// select from table/reference

			// validate supported targets as expressions or references table
			target_list_validate(&stmt->target_list, select->target);

			// table or reference table
			pushdown(self, stmt->ast);
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select (select from table)
			if (!select->target || !select->target->expr)
				error("FROM: undefined distributed table");

			// select from (select from table)
			auto from_expr = select->target->expr;
			if (from_expr->id != KSELECT)
				error("FROM: undefined distributed table");

			auto from_select = ast_select_of(from_expr);
			if (from_select->target == NULL ||
			    from_select->target->table == NULL)
				error("FROM SELECT: undefined distributed table");

			// validate FROM SELECT targets
			target_list_validate(&stmt->target_list, from_select->target);

			// generate pushdown as a nested query
			pushdown(self, from_expr);
			break;
		}

		// expression or reference table targets only

		// select (select from reference)
		// select expr(select from reference)
	
		// select pushdown to the first shard
		pushdown_first(self, stmt->ast);
		break;
	}

	default:
		abort();
		break;
	}

	// CRET
	op0(self, CRET);
}

static inline void
emit_send(Compiler* self, int start)
{
	Table* table = NULL;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
	{
		auto insert = ast_insert_of(stmt->ast);
		// CSEND
		auto table = (intptr_t)insert->target->table;
		op4(self, CSEND, stmt->order, start, table, insert->rows);
		break;
	}
	case STMT_UPDATE:
		table = ast_update_of(stmt->ast)->table;
		break;
	case STMT_DELETE:
		table = ast_delete_of(stmt->ast)->table;
		break;
	case STMT_SELECT:
	{
		// todo: point lookup

		// no targets or all targets are expressions
		if (target_list_expr(&stmt->target_list))
			break;

		// direct query from distributed or reference table
		auto select = ast_select_of(stmt->ast);
		if (select->target && select->target->table)
		{
			table = select->target->table;
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select from (select from table)
			auto from_expr = select->target->expr;
			auto from_select = ast_select_of(from_expr);
			table = from_select->target->table;
			break;
		}

		// nested expression or nested reference table targets

		// CSEND_FIRST
		op2(self, CSEND_FIRST, stmt->order, start);
		break;
	}

	default:
		abort();
		break;
	}

	if (! table)
		return;

	// CSEND_FIRST or CSEND_ALL
	if (table->config->reference)
		op2(self, CSEND_FIRST, stmt->order, start);
	else
		op2(self, CSEND_ALL, stmt->order, start);
}

static inline void
emit_recv(Compiler* self)
{
	int r = -1;
	auto stmt = self->current;
	switch (stmt->id) {
	case STMT_INSERT:
		op1(self, CRECV, stmt->order);
		break;

	case STMT_UPDATE:
		op1(self, CRECV, stmt->order);
		break;

	case STMT_DELETE:
		op1(self, CRECV, stmt->order);
		break;

	case STMT_SELECT:
	{
		// no targets or all targets are expressions
		if (target_list_expr(&stmt->target_list))
		{
			r = emit_select(self, stmt->ast);
			break;
		}

		// direct query from distributed or reference table
		auto select = ast_select_of(stmt->ast);
		if (select->target && select->target->table)
		{
			r = pushdown_recv(self, stmt->ast);
			break;
		}

		// nested distributed table query
		if (target_list_has(&stmt->target_list, TARGET_TABLE))
		{
			// select over returned SET or MERGE object
			auto from_expr = select->target->expr;
			select->target->rexpr = pushdown_recv(self, from_expr);
			r = emit_select(self, stmt->ast);
			break;
		}

		// nested expression or nested reference table targets
		// (first shard only, single result)

		// CRECV_TO
		r = op2(self, CRECV_TO, rpin(self), stmt->order);
		break;
	}

	default:
		abort();
		break;
	}

	if (r == -1)
	{
		// todo: add empty set?
		r = op1(self, CNULL, rpin(self));
	}

	// CCTE_SET
	op2(self, CCTE_SET, stmt->order, r);
	runpin(self, r);
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

	// analyze statements
	//
	// find last statement which access a table, mark as required
	// snapshot if there are two or more statements which access a table
	int count = 0;
	self->last = analyze_stmts(parser, &count);
	if (count >= 2)
		self->snapshot = true;

	// process statements
	auto recv_last = -1;
	list_foreach(&stmt_list->list)
	{
		auto stmt = list_at(Stmt, link);

		// generate shard code (pushdown)
		auto stmt_start = code_count(&self->code_shard);
		compiler_switch_shard(self);
		self->current = stmt;
		emit_stmt(self);

		// generate coordinator code
		compiler_switch_coordinator(self);

		// generate recv up to the max dependable statement order, including
		// coordinator only expressions
		auto stmt_expr = target_list_expr(&stmt->target_list);
		auto recv = -1;
		if (stmt_expr)
			recv = stmt->order;
		else
			recv = stmt_max_cte_order(stmt);
		if (recv >= 0)
		{
			emit_recv_upto(self, recv_last, recv);
			recv_last = recv;
		}

		// skip expression only statements (generated by recv above)
		if (stmt_expr)
			continue;

		// generate SEND command
		emit_send(self, stmt_start);
	}

	// recv rest of commands (if any left)
	emit_recv_upto(self, recv_last, stmt_list->list_count);

	// CBODY (for the main statement, if any)
	if (parser->stmt && parser->stmt->id == STMT_SELECT)
		op1(self, CBODY, parser->stmt->order);

	// CRET
	compiler_switch_coordinator(self);
	op0(self, CRET);
}
