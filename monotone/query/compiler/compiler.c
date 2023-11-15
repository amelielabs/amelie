
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>

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

bool
compiler_is_utility(Compiler* self)
{
	if (self->parser.stmts_count != 1)
		return false;

	auto stmt = compiler_first(self);
	switch (stmt->id) {
	case STMT_SHOW:
	case STMT_SET:
	case STMT_CREATE_USER:
	case STMT_CREATE_SCHEMA:
	case STMT_CREATE_TABLE:
	case STMT_CREATE_VIEW:
	case STMT_DROP_USER:
	case STMT_DROP_SCHEMA:
	case STMT_DROP_TABLE:
	case STMT_DROP_VIEW:
	case STMT_ALTER_USER:
	case STMT_ALTER_SCHEMA:
	case STMT_ALTER_TABLE:
	case STMT_ALTER_VIEW:
		return true;
	default:
		break;
	}
	return false;
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
			emit_update(self, stmt->ast);
			break;

		case STMT_DELETE:
			emit_delete(self, stmt->ast);
			break;

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
		default:
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
