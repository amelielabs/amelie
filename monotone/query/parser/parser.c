
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

void
parser_init(Parser* self, Db* db)
{
	self->explain     = EXPLAIN_NONE;
	self->stmts_count = 0;
	self->db          = db;
	list_init(&self->stmts);
	lex_init(&self->lex, keywords);
}

void
parser_reset(Parser* self)
{
	self->explain     = EXPLAIN_NONE;
	self->stmts_count = 0;
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
		case STMT_CREATE_SCHEMA:
		{
			auto ast = ast_schema_create_of(stmt->ast);
			if (ast->config)
				schema_config_free(ast->config);
			break;
		}
		case STMT_CREATE_TABLE:
		{
			auto ast = ast_table_create_of(stmt->ast);
			if (ast->config)
				table_config_free(ast->config);
			break;
		}
		case STMT_CREATE_VIEW:
		{
			auto ast = ast_view_create_of(stmt->ast);
			if (ast->config)
				view_config_free(ast->config);
			break;
		}
		default:
			break;
		}
	}
	list_init(&self->stmts);
	lex_reset(&self->lex);
}

bool
parser_has_utility(Parser* self)
{
	list_foreach(&self->stmts)
	{
		auto stmt = list_at(Stmt, link);
		switch (stmt->id) {
		case STMT_SHOW:
		case STMT_SET:
		case STMT_CHECKPOINT:
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
	}
	return false;
}
