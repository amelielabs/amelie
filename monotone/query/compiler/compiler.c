
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
compiler_switch(Compiler* self, bool coordinator)
{
	if (coordinator)
		self->code = &self->code_coordinator;
	else
		self->code = &self->code_stmt;
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
	// process statements
	list_foreach(&self->parser.stmts)
	{
		auto stmt = list_at(Stmt, link);
		self->current = stmt;

		compiler_switch(self, false);
		if (stmt->id == STMT_INSERT)
		{
			auto insert = ast_insert_of(stmt->ast);
			if (insert->on_conflict == ON_CONFLICT_NONE)
				emit_insert(self, stmt->ast);
			else
				emit_upsert(self, stmt->ast);
			compiler_switch(self, true);
			op0(self, CRECV);
			continue;
		}

		switch (stmt->id) {
		case STMT_UPDATE:
			emit_update(self, stmt->ast);
			dispatch_copy(self->dispatch, &self->code_stmt, stmt->order);

			compiler_switch(self, true);
			op0(self, CRECV);
			break;

		case STMT_DELETE:
			emit_delete(self, stmt->ast);
			dispatch_copy(self->dispatch, &self->code_stmt, stmt->order);

			compiler_switch(self, true);
			op0(self, CRECV);
			break;

		case STMT_SELECT:

			// if all targets are expressions execute locally
			if (target_list_is_expr(&stmt->target_list))
			{
				compiler_switch(self, true);
				emit_select(self, stmt->ast, false);

			} else
			{
				emit_select(self, stmt->ast, false);
				dispatch_copy(self->dispatch, &self->code_stmt, stmt->order);
				compiler_switch(self, true);
				op0(self, CRECV);
			}
			break;

		default:
			break;
		}

		// copy last generated statement
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
