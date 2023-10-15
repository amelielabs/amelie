
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

		compiler_set_code(self, &self->code_stmt);
		switch (stmt->id) {
		case STMT_UPDATE:
			break;
		case STMT_DELETE:
			break;
		case STMT_SELECT:
			emit_select(self, stmt->ast, false);
			break;
		default:
			assert(0);
			break;
		}

		// copy last generated statement
		self->iface->apply(0, &self->code_stmt, self->iface_arg);

		code_reset(&self->code_stmt);
	}

	// CRET
	self->iface->end(self->iface_arg);
	self->current = NULL;
}
