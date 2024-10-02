
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
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
#include <amelie_aggr.h>
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
		break;
	}
	case STMT_CREATE_INDEX:
	{
		auto ast = ast_index_create_of(stmt->ast);
		if (ast->config)
			index_config_free(ast->config);
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

hot static inline void
parse_stmt(Parser* self, Stmt* stmt)
{
	auto lex = &self->lex;
	auto ast = lex_next(lex);
	if (ast->id == KEOF)
		return;

	// RETURN cte_name | stmt
	if (ast->id == KRETURN)
	{
		stmt->ret = true;
		auto name = stmt_if(stmt, KNAME);
		if (name)
		{
			ast = lex_next(lex);
			auto is_last = ast->id == ';' || ast->id == KEOF;
			if (is_last)
			{
				auto cte = cte_list_find(&self->cte_list, &name->string);
				if (cte)
				{
					// cte
					stmt->id  = STMT_RETURN;
					stmt->cte = cte;
					lex_push(lex, ast);
					return;
				}
			}

			// handle as expression
			lex_push(lex, ast);
			lex_push(lex, name);
		}

		ast = lex_next(lex);
	}

	// cte_name := stmt | expr
	bool assign = false;
	if (ast->id == KNAME)
	{
		if (lex_if(lex, KASSIGN))
		{
			lex_push(lex, ast);
			parse_cte(stmt, false, false);
			assign = true;
			ast = lex_next(lex);
		}
	}

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

	case KPROMOTE:
		// PROMOTE id | RESET
		stmt->id = STMT_PROMOTE;
		parse_repl_promote(stmt);
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
		// [UNIQUE | SHARED | DISTRIBUTED | COMPUTE]
		bool unique  = false;
		bool shared  = false;
		bool compute = false;
		auto mod = lex_next(lex);
		switch (mod->id) {
		case KUNIQUE:
		{
			auto next = stmt_if(stmt, KINDEX);
			if (! next)
				error("CREATE UNIQUE <INDEX> expected");
			stmt_push(stmt, next);
			unique = true;
			break;
		}
		case KSHARED:
		{
			auto next = stmt_if(stmt, KTABLE);
			if (! next)
				error("CREATE SHARED <TABLE> expected");
			stmt_push(stmt, next);
			shared = true;
			break;
		}
		case KDISTRIBUTED:
		{
			auto next = stmt_if(stmt, KTABLE);
			if (! next)
				error("CREATE DISTRIBUTED <TABLE> expected");
			stmt_push(stmt, next);
			shared = false;
			break;
		}
		case KCOMPUTE:
		{
			auto next = stmt_if(stmt, KNODE);
			if (! next)
				error("CREATE COMPUTE <NODE> expected");
			stmt_push(stmt, next);
			compute = false;
			break;
		}
		default:
			stmt_push(stmt, mod);
			break;
		}

		// CREATE USER | REPLICA | NODE | SCHEMA | TABLE | INDEX | VIEW
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_CREATE_USER;
			parse_user_create(stmt);
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
			parse_table_create(stmt, shared);
		} else
		if (lex_if(lex, KINDEX))
		{
			stmt->id = STMT_CREATE_INDEX;
			parse_index_create(stmt, unique);
		} else
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_CREATE_VIEW;
			parse_view_create(stmt);
		} else {
			error("CREATE <USER|REPLICA|NODE|SCHEMA|TABLE|INDEX|VIEW> expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | NODE | SCHEMA | TABLE | INDEX| VIEW
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
		} else
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_DROP_VIEW;
			parse_view_drop(stmt);
		} else {
			error("DROP <USER|REPLICA|NODE|SCHEMA|TABLE|INDEX|VIEW> expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | INDEX | VIEW
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
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_ALTER_VIEW;
			parse_view_alter(stmt);
		} else {
			error("ALTER <USER|SCHEMA|TABLE|INDEX|VIEW> expected");
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
		auto select = parse_select(stmt);
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
	{
		// SELECT expr (by default)
		lex_push(lex, ast);
		stmt->id = STMT_SELECT;
		auto select = parse_select_expr(stmt);
		stmt->ast = &select->ast;
		break;
	}
	}

	if (assign && stmt_is_utility(stmt))
		error(":= cannot be used with utility statements");
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
		                          self->data, &self->json,
		                          &self->stmt_list,
		                          &self->cte_list,
		                           self->args);
		stmt_list_add(&self->stmt_list, stmt);
		self->stmt = stmt;

		// name [(args)]
		parse_cte(stmt, true, true);

		// AS
		if (! lex_if(lex, KAS))
			error("WITH name <AS> expected");

		// (
		if (! lex_if(lex, '('))
			error("WITH name AS <(> expected");

		// stmt (cannot be a utility statement)
		parse_stmt(self, stmt);
		if (stmt_is_utility(stmt))
			error("CTE must DML or Select");

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
parse(Parser* self, Local* local, Columns* args, Str* str)
{
	self->local = local;
	self->args  = args;

	auto lex = &self->lex;
	lex_start(lex, str);

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
		                           self->data, &self->json,
		                           &self->stmt_list,
		                           &self->cte_list,
		                            self->args);
		stmt_list_add(&self->stmt_list, self->stmt);
		parse_stmt(self, self->stmt);

		// add nameless cte for the statement, if not used
		if (! self->stmt->cte)
			self->stmt->cte = cte_list_add(&self->cte_list, NULL, self->stmt->order);

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
	if (unlikely(self->explain && !self->stmt_list.list_count))
		error("EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (has_utility && self->stmt_list.list_count > 1)
		error("CTE and multi-statement utility commands are not supported");
}
