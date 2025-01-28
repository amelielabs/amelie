
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
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
parse_stmt_free(Stmt* stmt)
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
	case STMT_INSERT:
	{
		auto ast = ast_insert_of(stmt->ast);
		returning_free(&ast->ret);
		if (ast->values)
			set_cache_push(stmt->values_cache, ast->values);
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

	columns_free(&stmt->cte_args);
}

hot static inline void
parse_stmt(Parser* self, Stmt* stmt)
{
	auto lex = &self->lex;
	auto ast = lex_next(lex);
	if (ast->id == KEOF)
		return;

	switch (ast->id) {
	case KSHOW:
		// SHOW name
		stmt->id = STMT_SHOW;
		parse_show(stmt);
		break;

	case KSET:
		// SET name TO INT | STRING
		stmt->id = STMT_SET;
		parse_set(stmt);
		break;

	case KSUBSCRIBE:
		// SUBSCRIBE id
		stmt->id = STMT_SUBSCRIBE;
		parse_repl_subscribe(stmt);
		break;

	case KUNSUBSCRIBE:
		// UNSUBSCRIBE
		stmt->id = STMT_UNSUBSCRIBE;
		parse_repl_unsubscribe(stmt);
		break;

	case KSTART:
		// START REPL
		if (lex_if(lex, KREPL) ||
		    lex_if(lex, KREPLICATION))
		{
			stmt->id = STMT_START_REPL;
			parse_repl_start(stmt);
		} else {
			stmt_error(stmt, ast, "REPL expected");
		}
		break;

	case KSTOP:
		// STOP REPL
		if (lex_if(lex, KREPL) ||
		    lex_if(lex, KREPLICATION))
		{
			stmt->id = STMT_STOP_REPL;
			parse_repl_stop(stmt);
		} else {
			stmt_error(stmt, ast, "REPL expected");
		}
		break;

	case KCHECKPOINT:
		// CHECKPOINT
		stmt->id = STMT_CHECKPOINT;
		parse_checkpoint(stmt);
		break;

	case KCREATE:
	{
		// [UNIQUE | UNLOGGED | SHARED | DISTRIBUTED]
		bool unique      = false;
		bool unlogged    = false;
		bool shared      = false;
		bool distributed = false;
		for (auto stop = false; !stop ;)
		{
			auto mod = lex_next(lex);
			switch (mod->id) {
			case KUNIQUE:
				unique = true;
				break;
			case KUNLOGGED:
				unlogged = true;
				break;
			case KSHARED:
				shared = true;
				break;
			case KDISTRIBUTED:
				distributed = true;
				break;
			default:
				stmt_push(stmt, mod);
				stop = true;
				break;
			}
		}

		if (unique)
		{
			auto next = stmt_expect(stmt, KINDEX);
			stmt_push(stmt, next);
		}

		if (unlogged || shared || distributed)
		{
			auto next = stmt_expect(stmt, KTABLE);
			stmt_push(stmt, next);
		}

		// CREATE USER | TOKEN | REPLICA | SCHEMA | TABLE | INDEX
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_CREATE_USER;
			parse_user_create(stmt);
		} else
		if (lex_if(lex, KTOKEN))
		{
			stmt->id = STMT_CREATE_TOKEN;
			parse_token_create(stmt);
		} else
		if (lex_if(lex, KREPLICA))
		{
			stmt->id = STMT_CREATE_REPLICA;
			parse_replica_create(stmt);
		} else
		if (lex_if(lex, KSCHEMA))
		{
			stmt->id = STMT_CREATE_SCHEMA;
			parse_schema_create(stmt);
		} else
		if (lex_if(lex, KTABLE))
		{
			stmt->id = STMT_CREATE_TABLE;
			parse_table_create(stmt, unlogged, shared);
		} else
		if (lex_if(lex, KINDEX))
		{
			stmt->id = STMT_CREATE_INDEX;
			parse_index_create(stmt, unique);
		} else {
			stmt_error(stmt, NULL, "'USER|REPLICA|SCHEMA|TABLE|INDEX' expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | SCHEMA | TABLE | INDEX
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_DROP_USER;
			parse_user_drop(stmt);
		} else
		if (lex_if(lex, KREPLICA))
		{
			stmt->id = STMT_DROP_REPLICA;
			parse_replica_drop(stmt);
		} else
		if (lex_if(lex, KSCHEMA))
		{
			stmt->id = STMT_DROP_SCHEMA;
			parse_schema_drop(stmt);
		} else
		if (lex_if(lex, KTABLE))
		{
			stmt->id = STMT_DROP_TABLE;
			parse_table_drop(stmt);
		} else
		if (lex_if(lex, KINDEX))
		{
			stmt->id = STMT_DROP_INDEX;
			parse_index_drop(stmt);
		} else {
			stmt_error(stmt, NULL, "'USER|REPLICA|SCHEMA|TABLE|INDEX' expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX | COMPUTE
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_ALTER_USER;
			parse_user_alter(stmt);
		} else
		if (lex_if(lex, KSCHEMA))
		{
			stmt->id = STMT_ALTER_SCHEMA;
			parse_schema_alter(stmt);
		} else
		if (lex_if(lex, KTABLE))
		{
			stmt->id = STMT_ALTER_TABLE;
			parse_table_alter(stmt);
		} else
		if (lex_if(lex, KINDEX))
		{
			stmt->id = STMT_ALTER_INDEX;
			parse_index_alter(stmt);
		} else
		if (lex_if(lex, KCOMPUTE))
		{
			stmt->id = STMT_ALTER_COMPUTE;
			parse_compute_alter(stmt);
		} else {
			stmt_error(stmt, NULL, "'USER|SCHEMA|TABLE|INDEX|COMPUTE' expected");
		}
		break;
	}
	case KTRUNCATE:
	{
		stmt->id = STMT_TRUNCATE;
		parse_table_truncate(stmt);
		break;
	}

	case KINSERT:
		stmt->id = STMT_INSERT;
		parse_insert(stmt);
		break;

	case KUPDATE:
		stmt->id = STMT_UPDATE;
		parse_update(stmt);
		break;

	case KDELETE:
		stmt->id = STMT_DELETE;
		parse_delete(stmt);
		break;

	case KSELECT:
	{
		stmt->id = STMT_SELECT;
		auto select = parse_select(stmt, NULL, false);
		stmt->ast = &select->ast;
		break;
	}

	case KWATCH:
		// WATCH expr
		stmt->id = STMT_WATCH;
		parse_watch(stmt);
		break;

	case KBEGIN:
		break;

	case KCOMMIT:
		break;

	case KEOF:
		stmt_error(stmt, NULL, "unexpected end of statement");
		break;

	default:
		stmt_error(stmt, NULL, "unexpected statement");
		break;
	}

	if (stmt->ast)
	{
		stmt->ast->pos_start = ast->pos_start;
		stmt->ast->pos_end   = ast->pos_end;
	}

	// resolve select targets
	parse_select_resolve(stmt);
}

hot static bool
parse_with(Parser* self)
{
	auto lex = &self->lex;

	// WITH
	if (! lex_if(lex, KWITH))
		return false;

	for (;;)
	{
		// name [(args)] AS ( stmt )[, ...]
		auto stmt = stmt_allocate(self->db, self->function_mgr, self->local,
		                          &self->lex,
		                          self->data,
		                          self->values_cache,
		                          &self->json,
		                          &self->stmt_list,
		                           self->args);
		stmt_list_add(&self->stmt_list, stmt);
		self->stmt = stmt;

		// name [(args)]
		parse_cte(stmt);

		// AS (
		stmt_expect(stmt, KAS);
		auto start = stmt_expect(stmt, '(');

		// parse stmt (cannot be a utility statement)
		parse_stmt(self, stmt);

		// set cte returning columns
		Returning* ret = NULL;
		switch (stmt->id) {
		case STMT_INSERT:
			ret = &ast_insert_of(stmt->ast)->ret;
			break;
		case STMT_UPDATE:
			ret = &ast_update_of(stmt->ast)->ret;
			break;
		case STMT_DELETE:
			ret = &ast_delete_of(stmt->ast)->ret;
			break;
		case STMT_SELECT:
			ret = &ast_select_of(stmt->ast)->ret;
			break;
		default:
			stmt_error(stmt, start, "CTE statement must be DML or SELECT");
			break;
		}
		stmt->cte_columns = &ret->columns;

		// ensure that arguments count match
		if (stmt->cte_args.count > 0 &&
		    stmt->cte_args.count != stmt->cte_columns->count)
			stmt_error(stmt, start, "CTE arguments count does not match the returning arguments count");

		// )
		stmt_expect(stmt, ')');

		// ,
		if (! lex_if(lex, ','))
			break;
	}

	return true;
}

hot void
parse(Parser* self, Str* str)
{
	// prepare parser
	auto lex = &self->lex;
	lex_start(&self->lex, str);

	// [EXPLAIN]
	auto explain = lex_if(lex, KEXPLAIN);
	if (explain)
	{
		// EXPLAIN(PROFILE)
		self->explain = EXPLAIN;
		if (lex_if(lex, '('))
		{
			if (! lex_if(lex, KPROFILE))
				lex_error_expect(lex, lex_next(lex), KPROFILE);
			if (! lex_if(lex, ')'))
				lex_error_expect(lex, lex_next(lex), ')');
			self->explain |= EXPLAIN_PROFILE;
		}

	} else
	if (lex_if(lex, KPROFILE))
		self->explain = EXPLAIN|EXPLAIN_PROFILE;

	// [BEGIN]
	auto begin  = lex_if(lex, KBEGIN) != NULL;
	auto commit = false;

	// stmt [; stmt]
	bool has_utility = false;	
	for (;;)
	{
		// ; | EOF
		if (lex_if(lex, ';'))
			continue;
		if (lex_if(lex, KEOF))
			break;

		// BEGIN/COMMIT
		auto ast = lex_next(lex);
		switch (ast->id) {
		case KBEGIN:
			lex_error(lex, ast, "unexpected BEGIN operation");
			break;
		case KCOMMIT:
			if (! begin)
				lex_error(lex, ast, "unexpected COMMIT operation");
			commit = true;
			continue;
		default:
			if (commit)
				lex_error(lex, ast, "unexpected statement after COMMIT");
			lex_push(lex, ast);
			break;
		}

		// [WITH name AS ( cte )[, name AS (...)]]
		if (parse_with(self))
			continue;

		// stmt (last stmt is main)
		self->stmt = stmt_allocate(self->db, self->function_mgr,
		                           self->local,
		                           &self->lex,
		                           self->data,
		                           self->values_cache,
		                           &self->json,
		                           &self->stmt_list,
		                            self->args);
		stmt_list_add(&self->stmt_list, self->stmt);
		parse_stmt(self, self->stmt);

		if (stmt_is_utility(self->stmt))
			has_utility = true;

		// EOF
		if (lex_if(lex, KEOF))
			break;

		// ;
		stmt_expect(self->stmt, ';');
	}

	// [COMMIT]
	auto ast = lex_next(lex);
	if (begin && !commit)
		lex_error(lex, ast, "COMMIT expected at the end of transaction");

	// EOF
	if (ast->id != KEOF)
		lex_error(lex, ast, "unexpected token at the end of transaction");

	// force last statetement as return
	if (self->stmt)
		self->stmt->ret = true;

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !self->stmt_list.count))
		lex_error(lex, explain, "EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (has_utility && self->stmt_list.count > 1)
		lex_error(lex, ast, "multi-statement utility commands are not supported");

	// set statements order
	stmt_list_order(&self->stmt_list);
}
