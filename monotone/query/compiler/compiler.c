
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_request.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

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
	case STMT_CREATE_TABLE:
	case STMT_DROP_USER:
	case STMT_DROP_TABLE:
	case STMT_ALTER_USER:
	case STMT_ALTER_TABLE:
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

		if (stmt->id == STMT_INSERT)
		{
			emit_insert(self, stmt->ast);
			continue;
		}

		switch (stmt->id) {
		case STMT_UPDATE:
			emit_update(self, stmt->ast);
			req_set_copy(self->req_set, &self->code);
			break;
		case STMT_DELETE:
			emit_delete(self, stmt->ast);
			req_set_copy(self->req_set, &self->code);
			break;
		case STMT_SELECT:
			emit_select(self, stmt->ast, false);

			req_set_copy(self->req_set, &self->code);
			break;
		default:
			assert(0);
			break;
		}

		// copy last generated statement
		code_reset(&self->code);
	}

	// CRET
	auto req_set = self->req_set;
	for (int i = 0; i < req_set->set_size; i++)
	{
		auto req = req_set_at(req_set, i);
		if (! req)
			continue;
		code_add(&req->code, CRET, 0, 0, 0, 0);
	}
	self->current = NULL;
}
