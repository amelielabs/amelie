
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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
		if (ast->config)
			replica_config_free(ast->config);
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
		if (ast->values)
			set_cache_push(stmt->parser->values_cache, ast->values);
		break;
	}
	default:
		break;
	}
	if (stmt->ret)
	{
		returning_free(stmt->ret);
		stmt->ret = NULL;
	}
	columns_free(&stmt->cte_columns);

	// free select statements
	for (auto ref = stmt->select_list.list; ref; ref = ref->next)
	{
		auto select = ast_select_of(ref->ast);
		returning_free(&select->ret);
		columns_free(&select->from_group_columns);
	}
}

hot void
parse_stmt(Stmt* self)
{
	// [RETURN var | stmt]
	auto return_ = stmt_if(self, KRETURN);
	if (return_)
	{
		self->is_return = true;
		auto name = stmt_if(self, KNAME);
		if (name)
		{
			stmt_push(self, name);
			stmt_push(self, return_);
		}
	}

	// stmt
	auto ast = stmt_next(self);
	switch (ast->id) {
	case KSHOW:
		// SHOW name
		self->id = STMT_SHOW;
		parse_show(self);
		break;

	case KSUBSCRIBE:
		// SUBSCRIBE id
		self->id = STMT_SUBSCRIBE;
		parse_repl_subscribe(self);
		break;

	case KUNSUBSCRIBE:
		// UNSUBSCRIBE
		self->id = STMT_UNSUBSCRIBE;
		parse_repl_unsubscribe(self);
		break;

	case KSTART:
		// START REPL
		if (stmt_if(self, KREPL) ||
		    stmt_if(self, KREPLICATION))
		{
			self->id = STMT_START_REPL;
			parse_repl_start(self);
		} else {
			stmt_error(self, ast, "REPL expected");
		}
		break;

	case KSTOP:
		// STOP REPL
		if (stmt_if(self, KREPL) ||
		    stmt_if(self, KREPLICATION))
		{
			self->id = STMT_STOP_REPL;
			parse_repl_stop(self);
		} else {
			stmt_error(self, ast, "REPL expected");
		}
		break;

	case KCHECKPOINT:
		// CHECKPOINT
		self->id = STMT_CHECKPOINT;
		parse_checkpoint(self);
		break;

	case KCREATE:
	{
		// [UNIQUE | UNLOGGED]
		bool unique   = false;
		bool unlogged = false;
		for (auto stop = false; !stop ;)
		{
			auto mod = stmt_next(self);
			switch (mod->id) {
			case KUNIQUE:
				unique = true;
				break;
			case KUNLOGGED:
				unlogged = true;
				break;
			default:
				stmt_push(self, mod);
				stop = true;
				break;
			}
		}

		if (unique)
		{
			auto next = stmt_expect(self, KINDEX);
			stmt_push(self, next);
		}

		if (unlogged)
		{
			auto next = stmt_expect(self, KTABLE);
			stmt_push(self, next);
		}

		// CREATE USER | TOKEN | REPLICA | SCHEMA | TABLE | INDEX | PROCEDURE
		if (stmt_if(self, KUSER))
		{
			self->id = STMT_CREATE_USER;
			parse_user_create(self);
		} else
		if (stmt_if(self, KTOKEN))
		{
			self->id = STMT_CREATE_TOKEN;
			parse_token_create(self);
		} else
		if (stmt_if(self, KREPLICA))
		{
			self->id = STMT_CREATE_REPLICA;
			parse_replica_create(self);
		} else
		if (stmt_if(self, KSCHEMA))
		{
			self->id = STMT_CREATE_SCHEMA;
			parse_schema_create(self);
		} else
		if (stmt_if(self, KTABLE))
		{
			self->id = STMT_CREATE_TABLE;
			parse_table_create(self, unlogged);
		} else
		if (stmt_if(self, KINDEX))
		{
			self->id = STMT_CREATE_INDEX;
			parse_index_create(self, unique);
		} else
		if (stmt_if(self, KPROCEDURE))
		{
			self->id = STMT_CREATE_PROCEDURE;
			parse_procedure_create(self, false);
		} else
		{
			stmt_error(self, NULL, "USER|REPLICA|SCHEMA|TABLE|INDEX|PROCEDURE expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | SCHEMA | TABLE | INDEX | PROCEDURE
		if (stmt_if(self, KUSER))
		{
			self->id = STMT_DROP_USER;
			parse_user_drop(self);
		} else
		if (stmt_if(self, KREPLICA))
		{
			self->id = STMT_DROP_REPLICA;
			parse_replica_drop(self);
		} else
		if (stmt_if(self, KSCHEMA))
		{
			self->id = STMT_DROP_SCHEMA;
			parse_schema_drop(self);
		} else
		if (stmt_if(self, KTABLE))
		{
			self->id = STMT_DROP_TABLE;
			parse_table_drop(self);
		} else
		if (stmt_if(self, KINDEX))
		{
			self->id = STMT_DROP_INDEX;
			parse_index_drop(self);
		} else
		if (stmt_if(self, KPROCEDURE))
		{
			self->id = STMT_DROP_PROCEDURE;
			parse_procedure_drop(self);
		} else {
			stmt_error(self, NULL, "USER|REPLICA|SCHEMA|TABLE|INDEX|PROCEDURE expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX | PROCEDURE
		if (stmt_if(self, KUSER))
		{
			self->id = STMT_ALTER_USER;
			parse_user_alter(self);
		} else
		if (stmt_if(self, KSCHEMA))
		{
			self->id = STMT_ALTER_SCHEMA;
			parse_schema_alter(self);
		} else
		if (stmt_if(self, KTABLE))
		{
			self->id = STMT_ALTER_TABLE;
			parse_table_alter(self);
		} else
		if (stmt_if(self, KINDEX))
		{
			self->id = STMT_ALTER_INDEX;
			parse_index_alter(self);
		} else
		if (stmt_if(self, KPROCEDURE))
		{
			self->id = STMT_ALTER_PROCEDURE;
			parse_procedure_alter(self);
		} else {
			stmt_error(self, NULL, "USER|SCHEMA|TABLE|INDEX|PROCEDURE expected");
		}
		break;
	}

	case KTRUNCATE:
	{
		self->id = STMT_TRUNCATE;
		parse_table_truncate(self);
		break;
	}

	case KWATCH:
		// WATCH expr
		self->id = STMT_WATCH;
		parse_watch(self);
		break;

	case KINSERT:
		self->id  = STMT_INSERT;
		parse_insert(self);
		self->block->stmts.last_send = self;
		break;

	case KUPDATE:
		self->id = STMT_UPDATE;
		parse_update(self);
		self->block->stmts.last_send = self;
		break;

	case KDELETE:
		self->id = STMT_DELETE;
		parse_delete(self);
		self->block->stmts.last_send = self;
		break;

	case KSELECT:
	{
		self->id = STMT_SELECT;
		auto select = parse_select(self, self->block->from, false);
		self->ast = &select->ast;
		self->ret = &select->ret;
		if (select->pushdown)
			self->block->stmts.last_send = self;
		break;
	}

	case KIF:
	{
		self->id = STMT_IF;
		if (parse_if(self))
			self->block->stmts.last_send = self;
		break;
	}

	case KFOR:
	{
		self->id = STMT_FOR;
		if (parse_for(self))
			self->block->stmts.last_send = self;
		break;
	}

	case KRETURN:
	{
		// RETURN var
		self->id = STMT_RETURN;
		parse_return(self);
		break;
	}

	case KBEGIN:
	case KCOMMIT:
		stmt_error(self, NULL, "unsupported statement");
		break;

	case KEOF:
		stmt_error(self, NULL, "unexpected end of statement");
		break;

	default:
		stmt_error(self, NULL, "unexpected statement");
		break;
	}

	// ensure statement can RETURN
	if (self->is_return && self->id != STMT_RETURN)
		if (! self->ret)
			stmt_error(self, ast, "RETURN statement must return data");

	if (self->ast)
	{
		self->ast->pos_start = ast->pos_start;
		self->ast->pos_end   = ast->pos_end;
	}

	// resolve select targets
	parse_select_resolve(self);
}

hot void
parse_block(Parser* self, Block* block)
{
	// stmt [; stmt]
	auto lex = &self->lex;
	for (;;)
	{
		// ;
		auto ast = lex_next(lex);
		if (ast->id == ';')
			continue;

		switch (ast->id) {
		case KWITH:
			// [WITH name AS ( cte )[, name AS (...)]]
			parse_with(self, block);
			break;
		case KDECLARE:
			// [DECLARE var type ;]
			// [DECLARE var type := expr]
			if (block != block->ns->blocks.list)
				lex_error(lex, ast, "DECLARE cannot be used here");
			parse_declare(self, block);
			break;
		case KNAME:
			// var := expr
			lex_push(lex, ast);
			parse_assign(self, block);
			break;

		case KEND:
		case KELSE:
		case KELSIF:
			// block end
			lex_push(lex, ast);
			return;
		case KEOF:
			return;

		default:
		{
			// stmt
			lex_push(lex, ast);
			auto stmt = stmt_allocate(self, lex, block);
			stmts_add(&block->stmts, stmt);
			parse_stmt(stmt);

			// validate usage of utility statement
			if (stmt_is_utility(stmt))
			{
				// ensure root block being used
				if (block != block->ns->blocks.list || block->ns != self->nss.list)
					stmt_error(stmt, stmt->ast, "utility statement cannot be used here");
				block->stmts.count_utility++;
			}
			break;
		}
		}

		// EOF
		if (lex_if(lex, KEOF))
			break;

		// ;
		lex_expect(lex, ';');
	}
}

hot void
parse(Parser* self, Program* program, Str* str)
{
	self->program = program;

	// prepare parser
	auto lex = &self->lex;
	lex_start(&self->lex, str);

	// [EXPLAIN]
	auto explain = lex_if(lex, KEXPLAIN);
	if (explain)
	{
		// EXPLAIN(PROFILE)
		self->program->explain = true;
		if (lex_if(lex, '('))
		{
			lex_expect(lex, KPROFILE);
			lex_expect(lex, ')');
			self->program->explain = false;
			self->program->profile = true;
		}

	} else
	if (lex_if(lex, KPROFILE))
		self->program->profile = true;

	// [BEGIN]
	auto begin = lex_if(lex, KBEGIN) != NULL;

	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL);
	auto block = blocks_add(&ns->blocks, NULL);

	// stmt [; stmt]
	parse_block(self, block);

	// [END [;]]
	if (begin)
	{
		lex_expect(lex, KEND);
		lex_if(lex, ';');
	}

	// EOF
	auto ast = lex_next(lex);
	if (ast->id != KEOF)
		lex_error(lex, ast, "unexpected token at the end of transaction");

	// ensure EXPLAIN has a command
	if (unlikely(! block->stmts.count))
		if (self->program->explain || self->program->profile)
			lex_error(lex, explain, "EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (block->stmts.count_utility && block->stmts.count > 1)
		lex_error(lex, ast, "multi-statement utility commands are not supported");

	// mark last stmt as return
	auto last = block->stmts.list_tail;
	if (last && !last->is_return)
		last->is_return = true;
}
