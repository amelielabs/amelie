
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

void
parser_init(Parser* self, Db* db)
{
	self->in_transaction = false;
	self->complete       = false;
	self->explain        = EXPLAIN_NONE;
	self->stmts_count    = 0;
	self->db             = db;
	list_init(&self->stmts);
	lex_init(&self->lex, keywords);
}

void
parser_reset(Parser* self)
{
	self->in_transaction = false;
	self->complete       = false;
	self->explain        = EXPLAIN_NONE;
	self->stmts_count    = 0;
	list_foreach(&self->stmts)
	{
		auto stmt = list_at(Stmt, link);
		switch (stmt->id) {
		case STMT_CREATE_USER:
		{
			auto ast = ast_user_create_of(stmt->ast);
			if (ast->config)
				user_config_free(ast->config);
			break;
		}
		case STMT_ALTER_USER:
		{
			auto ast = ast_user_alter_of(stmt->ast);
			if (ast->config)
				user_config_free(ast->config);
			break;
		}
		case STMT_CREATE_TABLE:
		{
			auto ast = ast_table_create_of(stmt->ast);
			if (ast->config)
				table_config_free(ast->config);
			break;
		}
		case STMT_ALTER_TABLE:
		{
			// todo:
			break;
		}
		default:
			break;
		}
	}
	list_init(&self->stmts);
	lex_reset(&self->lex);
}
