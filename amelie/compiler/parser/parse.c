
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

hot static inline void
parse_stmt(Scope* self)
{
	auto stmt = self->stmt;

	// RETURN expr | stmt
	auto ast = scope_next(self);
	if (ast->id == KRETURN)
	{
		if (stmt->cte)
			scope_error(self, ast, "RETURN cannot be used with CTE");
		if (stmt->assign)
			scope_error(self, ast, "RETURN cannot be used with := operator");
		stmt->ret = true;
		ast = scope_next(self);
	}

	switch (ast->id) {
	case KSHOW:
		// SHOW name
		stmt->id = STMT_SHOW;
		parse_show(self);
		break;

	case KSUBSCRIBE:
		// SUBSCRIBE id
		stmt->id = STMT_SUBSCRIBE;
		parse_repl_subscribe(self);
		break;

	case KUNSUBSCRIBE:
		// UNSUBSCRIBE
		stmt->id = STMT_UNSUBSCRIBE;
		parse_repl_unsubscribe(self);
		break;

	case KSTART:
		// START REPL
		if (scope_if(self, KREPL) ||
		    scope_if(self, KREPLICATION))
		{
			stmt->id = STMT_START_REPL;
			parse_repl_start(self);
		} else {
			scope_error(self, ast, "REPL expected");
		}
		break;

	case KSTOP:
		// STOP REPL
		if (scope_if(self, KREPL) ||
		    scope_if(self, KREPLICATION))
		{
			stmt->id = STMT_STOP_REPL;
			parse_repl_stop(self);
		} else {
			scope_error(self, ast, "REPL expected");
		}
		break;

	case KCHECKPOINT:
		// CHECKPOINT
		stmt->id = STMT_CHECKPOINT;
		parse_checkpoint(self);
		break;

	case KCREATE:
	{
		// [UNIQUE | UNLOGGED | OR REPLACE]
		bool unique     = false;
		bool unlogged   = false;
		bool or_replace = false;
		for (auto stop = false; !stop ;)
		{
			auto mod = scope_next(self);
			switch (mod->id) {
			case KUNIQUE:
				unique = true;
				break;
			case KUNLOGGED:
				unlogged = true;
				break;
			case KOR:
			{
				scope_expect(self, KREPLACE);
				auto fn = scope_expect(self, KFUNCTION);
				scope_push(self, fn);
				or_replace = true;
				break;
			}
			default:
				scope_push(self, mod);
				stop = true;
				break;
			}
		}

		if (unique)
		{
			auto next = scope_expect(self, KINDEX);
			scope_push(self, next);
		}

		if (unlogged)
		{
			auto next = scope_expect(self, KTABLE);
			scope_push(self, next);
		}

		// CREATE USER | TOKEN | REPLICA | SCHEMA | TABLE | INDEX | FUNCTION
		if (scope_if(self, KUSER))
		{
			stmt->id = STMT_CREATE_USER;
			parse_user_create(self);
		} else
		if (scope_if(self, KTOKEN))
		{
			stmt->id = STMT_CREATE_TOKEN;
			parse_token_create(self);
		} else
		if (scope_if(self, KREPLICA))
		{
			stmt->id = STMT_CREATE_REPLICA;
			parse_replica_create(self);
		} else
		if (scope_if(self, KSCHEMA))
		{
			stmt->id = STMT_CREATE_SCHEMA;
			parse_schema_create(self);
		} else
		if (scope_if(self, KTABLE))
		{
			stmt->id = STMT_CREATE_TABLE;
			parse_table_create(self, unlogged);
		} else
		if (scope_if(self, KINDEX))
		{
			stmt->id = STMT_CREATE_INDEX;
			parse_index_create(self, unique);
		} else
		if (scope_if(self, KFUNCTION))
		{
			stmt->id = STMT_CREATE_FUNCTION;
			parse_function_create(self, or_replace);
		} else {
			scope_error(self, NULL, "'USER|REPLICA|SCHEMA|TABLE|INDEX|FUNCTION' expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | SCHEMA | TABLE | INDEX | FUNCTION
		if (scope_if(self, KUSER))
		{
			stmt->id = STMT_DROP_USER;
			parse_user_drop(self);
		} else
		if (scope_if(self, KREPLICA))
		{
			stmt->id = STMT_DROP_REPLICA;
			parse_replica_drop(self);
		} else
		if (scope_if(self, KSCHEMA))
		{
			stmt->id = STMT_DROP_SCHEMA;
			parse_schema_drop(self);
		} else
		if (scope_if(self, KTABLE))
		{
			stmt->id = STMT_DROP_TABLE;
			parse_table_drop(self);
		} else
		if (scope_if(self, KINDEX))
		{
			stmt->id = STMT_DROP_INDEX;
			parse_index_drop(self);
		} else
		if (scope_if(self, KFUNCTION))
		{
			stmt->id = STMT_DROP_FUNCTION;
			parse_function_drop(self);
		} else {
			scope_error(self, NULL, "'USER|REPLICA|SCHEMA|TABLE|INDEX|FUNCTION' expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX | FUNCTION
		if (scope_if(self, KUSER))
		{
			stmt->id = STMT_ALTER_USER;
			parse_user_alter(self);
		} else
		if (scope_if(self, KSCHEMA))
		{
			stmt->id = STMT_ALTER_SCHEMA;
			parse_schema_alter(self);
		} else
		if (scope_if(self, KTABLE))
		{
			stmt->id = STMT_ALTER_TABLE;
			parse_table_alter(self);
		} else
		if (scope_if(self, KINDEX))
		{
			stmt->id = STMT_ALTER_INDEX;
			parse_index_alter(self);
		} else
		if (scope_if(self, KFUNCTION))
		{
			stmt->id = STMT_ALTER_FUNCTION;
			parse_function_alter(self);
		} else {
			scope_error(self, NULL, "'USER|SCHEMA|TABLE|INDEX|FUNCTION' expected");
		}
		break;
	}

	case KTRUNCATE:
	{
		stmt->id = STMT_TRUNCATE;
		parse_table_truncate(self);
		break;
	}

	case KINSERT:
		stmt->id = STMT_INSERT;
		parse_insert(self);
		break;

	case KUPDATE:
		stmt->id = STMT_UPDATE;
		parse_update(self);
		break;

	case KDELETE:
		stmt->id = STMT_DELETE;
		parse_delete(self);
		break;

	case KSELECT:
	{
		stmt->id = STMT_SELECT;
		auto select = parse_select(self, NULL, false);
		stmt->ast = &select->ast;
		break;
	}

	case KWATCH:
		// WATCH expr
		stmt->id = STMT_WATCH;
		parse_watch(self);
		break;

	case KBEGIN:
		break;

	case KCOMMIT:
		break;

	case KEOF:
		scope_error(self, NULL, "unexpected end of statement");
		break;

	default:
		// var := expr or RETURN expr
		if (stmt->assign || stmt->ret)
		{
			// SELECT expr
			scope_push(self, ast);
			stmt->id = STMT_SELECT;
			auto select = parse_select_expr(self);
			stmt->ast = &select->ast;
			break;
		}
		scope_error(self, NULL, "unexpected statement");
		break;
	}

	if (stmt->ast)
	{
		stmt->ast->pos_start = ast->pos_start;
		stmt->ast->pos_end   = ast->pos_end;
	}

	// resolve select targets
	parse_select_resolve(self);
}

hot static void
parse_with(Scope* self)
{
	for (;;)
	{
		// name [(args)] AS ( stmt )[, ...]
		auto stmt = stmt_allocate(self);
		stmts_add(&self->stmts, stmt);
		self->stmt = stmt;

		// name [(args)]
		parse_cte(self);

		// AS (
		scope_expect(self, KAS);
		auto start = scope_expect(self, '(');

		// parse stmt (cannot be a utility statement)
		parse_stmt(self);

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
			scope_error(self, start, "CTE statement must be DML or SELECT");
			break;
		}
		stmt->cte->columns = &ret->columns;

		// ensure that arguments count match
		if (stmt->cte->args.count > 0 &&
		    stmt->cte->args.count != stmt->cte->columns->count)
			scope_error(self, start, "CTE arguments count does not match the returning arguments count");

		// )
		scope_expect(self, ')');

		// ,
		if (! scope_if(self, ','))
			break;
	}
}

hot static void
parse_scope(Scope* self)
{
	// stmt [; stmt]
	for (;;)
	{
		// ; | EOF
		if (scope_if(self, ';'))
			continue;
		if (scope_if(self, KEOF))
			break;

		// [BEGIN/COMMIT]
		auto ast = scope_next(self);
		switch (ast->id) {
		case KBEGIN:
			scope_error(self, ast, "unexpected BEGIN operation");
			break;
		case KCOMMIT:
			if (! self->parser->begin)
				scope_error(self, ast, "unexpected COMMIT operation");
			self->parser->commit = true;
			continue;
		default:
			if (self->parser->commit)
				scope_error(self, ast, "unexpected statement after COMMIT");
			scope_push(self, ast);
			break;
		}

		// name := expr/stmt
		Var* assign = NULL;
		ast = scope_if(self, KNAME);
		if (ast)
		{
			scope_expect(self, KASSIGN);
			assign = vars_add(&self->vars, &ast->string);
		}

		// [WITH name AS ( cte )[, name AS (...)]]
		if (scope_if(self, KWITH))
			parse_with(self);

		ast = scope_if(self, KEOF);
		if (ast)
			scope_error(self, ast, "statement expected");

		// stmt (last stmt is main)
		auto stmt = stmt_allocate(self);
		self->stmt = stmt;
		stmts_add(&self->stmts, stmt);
		stmt->assign = assign;

		// stmt
		parse_stmt(self);

		if (stmt_is_utility(stmt) && stmt->assign)
			scope_error(self, ast, ":= cannot be used with utility statements");

		// EOF
		if (scope_if(self, KEOF))
			break;

		// ;
		scope_expect(self, ';');
	}

	// set statements order
	stmts_order(&self->stmts);
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
	auto scope = scope_allocate(self, NULL);
	scope->lex = &self->lex;
	scopes_add(&self->scopes, scope);
	parse_scope(scope);

	// [COMMIT]
	auto ast = lex_next(lex);
	if (self->begin && !self->commit)
		lex_error(lex, ast, "COMMIT expected at the end of transaction");

	// EOF
	if (ast->id != KEOF)
		lex_error(lex, ast, "unexpected token at the end of transaction");

	// force last statetement as return
	if (scope->stmt)
		scope->stmt->ret = true;

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !scope->stmts.count))
		lex_error(lex, explain, "EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (scope->stmts.count_utility && scope->stmts.count > 1)
		lex_error(lex, ast, "multi-statement utility commands are not supported");
}
