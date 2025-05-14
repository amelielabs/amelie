
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
	case STMT_CREATE_PROCEDURE:
	{
		auto ast = ast_procedure_create_of(stmt->ast);
		if (ast->config)
			proc_config_free(ast->config);
		break;
	}
	case STMT_INSERT:
	{
		auto ast = ast_insert_of(stmt->ast);
		returning_free(&ast->ret);
		if (ast->values)
			set_cache_push(stmt->parser->values_cache, ast->values);
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

hot static inline void
parse_stmt(Parser* self, Stmt* stmt)
{
	auto lex = &self->lex;
	auto ast = lex_next(lex);

	switch (ast->id) {
	case KSHOW:
		// SHOW name
		stmt->id = STMT_SHOW;
		parse_show(stmt);
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
		// [UNIQUE | UNLOGGED | OR REPLACE]
		bool unique     = false;
		bool unlogged   = false;
		bool or_replace = false;
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
			case KOR:
			{
				stmt_expect(stmt, KREPLACE);
				auto fn = stmt_expect(stmt, KPROCEDURE);
				stmt_push(stmt, fn);
				or_replace = true;
				break;
			}
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

		if (unlogged)
		{
			auto next = stmt_expect(stmt, KTABLE);
			stmt_push(stmt, next);
		}

		// CREATE USER | TOKEN | REPLICA | SCHEMA | TABLE | INDEX | PROCEDURE
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
			parse_table_create(stmt, unlogged);
		} else
		if (lex_if(lex, KINDEX))
		{
			stmt->id = STMT_CREATE_INDEX;
			parse_index_create(stmt, unique);
		} else
		if (lex_if(lex, KPROCEDURE))
		{
			stmt->id = STMT_CREATE_PROCEDURE;
			parse_procedure_create(stmt, or_replace);
		} else {
			stmt_error(stmt, NULL, "USER|REPLICA|SCHEMA|TABLE|INDEX|PROCEDURE expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | SCHEMA | TABLE | INDEX | PROCEDURE
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
		} else
		if (lex_if(lex, KPROCEDURE))
		{
			stmt->id = STMT_DROP_PROCEDURE;
			parse_procedure_drop(stmt);
		} else {
			stmt_error(stmt, NULL, "USER|REPLICA|SCHEMA|TABLE|INDEX|PROCEDURE expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX | PROCEDURE
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
		if (lex_if(lex, KPROCEDURE))
		{
			stmt->id = STMT_ALTER_PROCEDURE;
			parse_procedure_alter(stmt);
		} else {
			stmt_error(stmt, NULL, "USER|SCHEMA|TABLE|INDEX|PROCEDURE expected");
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
	case KCOMMIT:
		stmt_error(stmt, NULL, "unexpected statement");
		break;

	case KCALL:
	{
		stmt->id = STMT_CALL;
		parse_call(stmt);
		break;
	}

	case KEXECUTE:
	{
		if (self->execute)
			stmt_error(stmt, NULL, "EXECUTE statement can be used only once and cannot be mixed");
		stmt->id = STMT_EXECUTE;
		parse_execute(stmt);
		self->execute = true;
		break;
	}

	case KEOF:
		stmt_error(stmt, NULL, "unexpected end of statement");
		break;

	default:
		// var := expr
		if (stmt->assign)
		{
			// SELECT expr
			lex_push(lex, ast);
			stmt->id = STMT_SELECT;
			auto select = parse_select_expr(stmt);
			stmt->ast = &select->ast;
			break;
		}
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

hot static void
parse_with(Parser* self, Scope* scope)
{
	auto lex = &self->lex;
	for (;;)
	{
		// name [(args)] AS ( stmt )[, ...]
		auto stmt = stmt_allocate(self, &self->lex, scope);
		stmts_add(&self->stmts, stmt);
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
		stmt->cte->columns = &ret->columns;

		// ensure that arguments count match
		if (stmt->cte->args.count > 0 &&
		    stmt->cte->args.count != stmt->cte->columns->count)
			stmt_error(stmt, start, "CTE arguments count does not match the returning arguments count");

		// )
		stmt_expect(stmt, ')');

		// ,
		if (! lex_if(lex, ','))
			break;
	}
}

hot void
parse_scope(Parser* self, Scope* scope)
{
	// stmt [; stmt]
	auto lex = &self->lex;
	for (;;)
	{
		// ; | EOF
		if (lex_if(lex, ';'))
			continue;
		if (lex_if(lex, KEOF))
			break;

		// [COMMIT]
		auto ast = lex_next(lex);
		if (ast->id == KCOMMIT)
		{
			if (scope->call)
				lex_error(lex, ast, "COMMIT cannot be inside procedure");
			if (! self->begin)
				lex_error(lex, ast, "unexpected COMMIT");
			self->commit = true;
			// ;
			lex_if(lex, ';');
			break;
		}
		lex_push(lex, ast);

		// name := expr/stmt
		Var* assign = NULL;
		ast = lex_if(lex, KNAME);
		if (ast)
		{
			if (! lex_if(lex, KASSIGN))
				lex_error_expect(lex, lex_next(lex), KASSIGN);
			assign = vars_add(&scope->vars, &ast->string);
		}

		// [WITH name AS ( cte )[, name AS (...)]]
		if (lex_if(lex, KWITH))
			parse_with(self, scope);

		ast = lex_if(lex, KEOF);
		if (ast)
			lex_error(lex, ast, "statement expected");

		// stmt (last stmt is main)
		self->stmt = stmt_allocate(self, &self->lex, scope);
		stmts_add(&self->stmts, self->stmt);
		self->stmt->assign = assign;

		// stmt
		parse_stmt(self, self->stmt);

		if (stmt_is_utility(self->stmt))
		{
			if (self->stmt->assign)
				stmt_error(self->stmt, NULL, ":= cannot be used with utility statements");
			self->stmts.count_utility++;
		}

		// EOF
		if (lex_if(lex, KEOF))
			break;

		// ;
		stmt_expect(self->stmt, ';');
	}
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
	self->begin = lex_if(lex, KBEGIN) != NULL;

	// stmt [; stmt]
	auto scope = scopes_add(&self->scopes, NULL, NULL);
	parse_scope(self, scope);

	// [COMMIT]
	auto ast = lex_next(lex);
	if (self->begin && !self->commit)
		lex_error(lex, ast, "COMMIT expected at the end of transaction");

	// EOF
	if (ast->id != KEOF)
		lex_error(lex, ast, "unexpected token at the end of transaction");

	// force last statetement as return
	if (self->stmt)
		self->stmt->ret = true;

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !self->stmts.count))
		lex_error(lex, explain, "EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (self->stmts.count_utility && self->stmts.count > 1)
		lex_error(lex, ast, "multi-statement utility commands are not supported");

	// set statements order
	stmts_order(&self->stmts);
}
