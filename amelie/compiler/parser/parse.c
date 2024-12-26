
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
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
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
		} else
		{
			error("START <REPL> expected");
		}
		break;

	case KSTOP:
		// STOP REPL
		if (lex_if(lex, KREPL) ||
		    lex_if(lex, KREPLICATION))
		{
			stmt->id = STMT_STOP_REPL;
			parse_repl_stop(stmt);
		} else
		{
			error("STOP <REPL> expected");
		}
		break;

	case KCHECKPOINT:
		// CHECKPOINT
		stmt->id = STMT_CHECKPOINT;
		parse_checkpoint(stmt);
		break;

	case KCREATE:
	{
		// [UNIQUE | UNLOGGED | SHARED | DISTRIBUTED | COMPUTE]
		bool unique      = false;
		bool unlogged    = false;
		bool shared      = false;
		bool distributed = false;
		bool compute     = false;
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
			case KCOMPUTE:
				compute = true;
				break;
			default:
				stmt_push(stmt, mod);
				stop = true;
				break;
			}
		}

		if (unique)
		{
			auto next = stmt_if(stmt, KINDEX);
			if (! next)
				error("CREATE UNIQUE <INDEX> expected");
			stmt_push(stmt, next);
		}

		if (compute)
		{
			auto next = stmt_if(stmt, KNODE);
			if (! next)
				error("CREATE COMPUTE <NODE> expected");
			stmt_push(stmt, next);
		}

		if (unlogged || shared || distributed)
		{
			auto next = stmt_if(stmt, KTABLE);
			if (! next)
				error("CREATE [UNLOGGED] [SHARED | DISTRIBUTED] <TABLE> expected");
			stmt_push(stmt, next);
		}

		// CREATE USER | TOKEN | REPLICA | NODE | SCHEMA | TABLE | INDEX
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
		if (lex_if(lex, KNODE))
		{
			stmt->id = STMT_CREATE_NODE;
			parse_node_create(stmt, compute);
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
			error("CREATE <USER|REPLICA|NODE|SCHEMA|TABLE|INDEX> expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | NODE | SCHEMA | TABLE | INDEX
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
		if (lex_if(lex, KNODE))
		{
			stmt->id = STMT_DROP_NODE;
			parse_node_drop(stmt);
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
			error("DROP <USER|REPLICA|NODE|SCHEMA|TABLE|INDEX> expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX
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
		} else {
			error("ALTER <USER|SCHEMA|TABLE|INDEX> expected");
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
		error("unexpected end of statement");
		break;

	default:
		error("unexpected statement");
		break;
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

		// AS
		if (! lex_if(lex, KAS))
			error("WITH name <AS> expected");

		// (
		if (! lex_if(lex, '('))
			error("WITH name AS <(> expected");

		// parse stmt (cannot be a utility statement)
		parse_stmt(self, stmt);

		// set cte returning columns
		Returning* ret;
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
			error("CTE statement must be DML or SELECT");
			break;
		}
		stmt->cte_columns = &ret->columns;

		// ensure that arguments count match
		if (stmt->cte_args.count > 0 &&
		    stmt->cte_args.count != stmt->cte_columns->count)
			error("CTE arguments count must mismatch the returning arguments count");

		// )
		if (! lex_if(lex, ')'))
			error("WITH name AS (<)> expected");

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
	if (lex_if(lex, KEXPLAIN))
	{
		// EXPLAIN(PROFILE)
		self->explain = EXPLAIN;
		if (lex_if(lex, '('))
		{
			if (! lex_if(lex, KPROFILE))
				error("EXPLAIN (<PROFILE>) expected");
			if (! lex_if(lex, ')'))
				error("EXPLAIN (<)> expected");
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
		if (commit)
			error("unexpected statement after COMMIT");
		if (lex_if(lex, KBEGIN))
			error("unexpected BEGIN operation");
		if (lex_if(lex, KCOMMIT))
		{
			if (! begin)
				error("unexpected COMMIT operation");
			commit = true;
			continue;
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
		if (! lex_if(lex, ';'))
			error("unexpected token at the end of statement");
	}

	// [COMMIT]
	if (begin && !commit)
		error("COMMIT expected at the end of transaction");

	// EOF
	if (! lex_if(lex, KEOF))
		error("unexpected token at the end of statement");

	// force last statetement as return
	if (self->stmt)
		self->stmt->ret = true;

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !self->stmt_list.count))
		error("EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (has_utility && self->stmt_list.count > 1)
		error("CTE and multi-statement utility commands are not supported");

	// set statements order
	stmt_list_order(&self->stmt_list);
}
