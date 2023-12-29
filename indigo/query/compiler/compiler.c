
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>

void
compiler_init(Compiler*    self,
              Db*          db,
              FunctionMgr* function_mgr,
              Router*      router,
              Dispatch*    dispatch)
{
	self->current      = NULL;
	self->dispatch     = dispatch;
	self->router       = router;
	self->function_mgr = function_mgr;
	self->db           = db;
	self->code         = &self->code_stmt;
	code_init(&self->code_stmt);
	code_init(&self->code_coordinator);
	code_data_init(&self->code_data);
	parser_init(&self->parser, db);
	rmap_init(&self->map);
}

void
compiler_free(Compiler* self)
{
	parser_reset(&self->parser);
	code_free(&self->code_stmt);
	code_free(&self->code_coordinator);
	code_data_free(&self->code_data);
	rmap_free(&self->map);
}

void
compiler_reset(Compiler* self)
{
	self->code    = &self->code_stmt;
	self->current = NULL;
	code_reset(&self->code_stmt);
	code_reset(&self->code_coordinator);
	code_data_reset(&self->code_data);
	parser_reset(&self->parser);
	rmap_reset(&self->map);
}

void
compiler_switch(Compiler* self, Code* code)
{
	self->code = code;
}

hot void
compiler_distribute(Compiler* self)
{
	auto router = self->dispatch->router;
	for (int i = 0; i < router->set_size; i++)
	{
		// get route by order
		auto route = router_at(router, i);

		// add request to the route
		auto req = dispatch_add(self->dispatch, self->current->order, route);

		// copy and relocate current stmt code
		op_relocate(&req->code, &self->code_stmt);
	}
}

void
compiler_parse(Compiler* self, Str* text)
{
	parse(&self->parser, text);
}

void
compiler_emit(Compiler* self)
{
	// prepare request set
	dispatch_prepare(self->dispatch, self->parser.stmts_count);

	// process statements
	list_foreach(&self->parser.stmts)
	{
		auto stmt = list_at(Stmt, link);
		self->current = stmt;

		compiler_switch(self, &self->code_stmt);
		if (stmt->id == STMT_INSERT)
		{
			auto insert = ast_insert_of(stmt->ast);
			if (insert->on_conflict == ON_CONFLICT_NONE)
				emit_insert(self, stmt->ast);
			else
				emit_upsert(self, stmt->ast);
			continue;
		}

		switch (stmt->id) {
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
			if (target_list_expr_or_reference(&stmt->target_list))
			{
				//
				// no targets
				// all targets are expressions
				// all targets are expressions or reference tables
				//
				// select expr
				// select from [expr/reference]
				// select (select expr)
				// select (select from [expr/reference])
				// select (select expr) from [expr/reference]
				// select (select from [expr/reference]) from [expr/reference]
				//
				compiler_switch(self, &self->code_coordinator);
				emit_select(self, stmt->ast, false);

				// CRECV (advance dispatch stmt)
				op0(self, CRECV);

			} else
			{
				// select (select from table)
				// select (select from table) from expr
				auto select = ast_select_of(stmt->ast);
				if (! select->target)
					error("undefined distributed table");

				// select from (select from table)
				if (! select->target->table)
				{
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
					select->target->rexpr = pushdown(self, from_expr, true);

					// select over returned SET or MERGE object
					emit_select(self, stmt->ast, false);
					break;
				}

				// validate supported targets as expression or reference table
				target_list_validate(&stmt->target_list, select->target);

				// select [select] from table, ...
				int r = pushdown(self, stmt->ast, false);
				if (r == -1)
					break;
				op1(self, CSEND_SET, r);
				runpin(self, r);
			}

			break;
		}
		case STMT_ABORT:
		{
			// produce error
			op0(self, CABORT);

			// copy statement code to each shard
			compiler_distribute(self);

			// coordinator
			compiler_switch(self, &self->code_coordinator);

			// CRECV
			op0(self, CRECV);
			break;
		}

		default:
			abort();
			break;
		}

		code_reset(&self->code_stmt);
	}

	// CRET
	auto dispatch = self->dispatch;
	for (int order = 0; order < dispatch->set_size; order++)
	{
		auto req = dispatch_at(dispatch, order);
		if (! req)
			continue;
		code_add(&req->code, CRET, 0, 0, 0, 0);
	}
	code_add(&self->code_coordinator, CRET, 0, 0, 0, 0);
	self->current = NULL;
}
