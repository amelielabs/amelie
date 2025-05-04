
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
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

static void
parser_reset_stmt(Parser* self, Stmt* stmt)
{
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
	case STMT_CREATE_REPLICA:
	{
		auto ast = ast_replica_create_of(stmt->ast);
		remote_free(&ast->remote);
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
	case STMT_ALTER_TABLE:
	{
		auto ast = ast_table_alter_of(stmt->ast);
		if (ast->column)
			column_free(ast->column);
		if (ast->value_buf)
			buf_free(ast->value_buf);
		break;
	}
	case STMT_CREATE_INDEX:
	{
		auto ast = ast_index_create_of(stmt->ast);
		if (ast->config)
			index_config_free(ast->config);
		break;
	}
	case STMT_CREATE_FUNCTION:
	{
		auto ast = ast_function_create_of(stmt->ast);
		if (ast->config)
			udf_config_free(ast->config);
		break;
	}
	case STMT_INSERT:
	{
		auto ast = ast_insert_of(stmt->ast);
		returning_free(&ast->ret);
		if (ast->values)
			set_cache_push(self->values_cache, ast->values);
		break;
	}
	case STMT_DELETE:
	{
		auto ast = ast_delete_of(stmt->ast);
		returning_free(&ast->ret);
		break;
	}
	case STMT_UPDATE:
	{
		auto ast = ast_update_of(stmt->ast);
		returning_free(&ast->ret);
		break;
	}
	default:
		break;
	}

	// free select statements
	for (auto ref = stmt->select_list.list; ref; ref = ref->next)
	{
		auto select = ast_select_of(ref->ast);
		returning_free(&select->ret);
		columns_free(&select->targets_group_columns);
	}
}

static void
parser_reset_scope(Parser* self, Scope* scope)
{
	auto stmt = scope->stmts.list;
	while (stmt)
	{
		parser_reset_stmt(self, stmt);
		stmt = stmt->next;
	}
	ctes_free(&scope->ctes);
}

void
parser_reset(Parser* self)
{
	self->explain = EXPLAIN_NONE;
	self->begin   = false;
	self->commit  = false;
	auto scope = self->scopes.list;
	while (scope)
	{
		parser_reset_scope(self, scope);
		scope = scope->next;
	}
	scopes_init(&self->scopes);
	lex_reset(&self->lex);
	uri_reset(&self->uri);
	json_reset(&self->json);
}

void
parser_init(Parser*      self,
            Db*          db,
            Local*       local,
            FunctionMgr* function_mgr,
            SetCache*    values_cache,
            Program*     program)
{
	self->explain      = EXPLAIN_NONE;
	self->begin        = false;
	self->commit       = false;
	self->program      = program;
	self->values_cache = values_cache;
	self->function_mgr = function_mgr;
	self->local        = local;
	self->db           = db;
	scopes_init(&self->scopes);
	lex_init(&self->lex, keywords_alpha);
	uri_init(&self->uri);
	json_init(&self->json);
}

void
parser_free(Parser* self)
{
	parser_reset(self);
	uri_free(&self->uri);
	json_free(&self->json);
}
