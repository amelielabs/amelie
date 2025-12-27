
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
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
	case STMT_CREATE_DB:
	{
		auto ast = ast_db_create_of(stmt->ast);
		if (ast->config)
			db_config_free(ast->config);
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
	case STMT_WHILE:
	{
		auto ast = ast_while_of(stmt->ast);
		buf_free(&ast->breaks);
		buf_free(&ast->continues);
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

hot static bool
parse_stmt_return(Stmt* self)
{
	auto return_ = stmt_if(self, KRETURN);
	if (! return_)
		return false;

	self->is_return = true;

	// ensure return called by function
	if (! self->block->ns->udf)
		stmt_error(self, return_, "RETURN can be used only within UDF");

	// RETURN var;
	auto name = stmt_if(self, KNAME);
	if (name)
	{
		auto semicolon = stmt_if(self, ';');
		if (semicolon)
		{
			// find variable by name
			auto var = namespace_find_var(self->block->ns, &name->string);
			if (! var)
				stmt_error(self, name, "variable not found");
			name->id  = KVAR;
			name->var = var;
			if (var->writer)
				deps_add_var(&self->deps, var->writer, var);

			self->id  = STMT_RETURN;
			self->ast = name;
			stmt_push(self, semicolon);
			return true;
		}
		stmt_push(self, name);
	}

	// RETURN;
	auto semicolon = stmt_if(self, ';');
	if (semicolon)
	{
		self->id = STMT_RETURN;
		stmt_push(self, semicolon);
		return true;
	}

	return false;
}

hot void
parse_stmt(Stmt* self)
{
	// [RETURN var | stmt | expr | ;]
	if (parse_stmt_return(self))
		return;

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
		bool unique     = false;
		bool unlogged   = false;
		bool or_replace = false;
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
			case KOR:
				stmt_expect(self, KREPLACE);
				or_replace = true;
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

		if (or_replace)
		{
			auto next = stmt_expect(self, KFUNCTION);
			stmt_push(self, next);
		}

		// CREATE USER | TOKEN | REPLICA | DATABASE | TABLE | INDEX | FUNCTION
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
		if (stmt_if(self, KDATABASE))
		{
			self->id = STMT_CREATE_DB;
			parse_db_create(self);
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
		if (stmt_if(self, KFUNCTION))
		{
			self->id = STMT_CREATE_FUNCTION;
			parse_function_create(self, or_replace);
		} else
		{
			stmt_error(self, NULL, "USER|REPLICA|DATABASE|TABLE|INDEX|FUNCTION expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | REPLICA | DATABASE | TABLE | INDEX | FUNCTION
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
		if (stmt_if(self, KDATABASE))
		{
			self->id = STMT_DROP_DB;
			parse_db_drop(self);
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
		if (stmt_if(self, KFUNCTION))
		{
			self->id = STMT_DROP_FUNCTION;
			parse_function_drop(self);
		} else {
			stmt_error(self, NULL, "USER|REPLICA|DATABASE|TABLE|INDEX|FUNCTION expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | DATABASE | TABLE | INDEX | FUNCTION
		if (stmt_if(self, KUSER))
		{
			self->id = STMT_ALTER_USER;
			parse_user_alter(self);
		} else
		if (stmt_if(self, KDATABASE))
		{
			self->id = STMT_ALTER_DB;
			parse_db_alter(self);
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
		if (stmt_if(self, KFUNCTION))
		{
			self->id = STMT_ALTER_FUNCTION;
			parse_function_alter(self);
		} else {
			stmt_error(self, NULL, "USER|DATABASE|TABLE|INDEX|FUNCTION expected");
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

	case KWHILE:
	{
		self->id = STMT_WHILE;
		if (parse_while(self))
			self->block->stmts.last_send = self;
		break;
	}

	case KBREAK:
	{
		self->id = STMT_BREAK;
		parse_break(self);
		break;
	}

	case KCONTINUE:
	{
		self->id = STMT_CONTINUE;
		parse_break(self);
		break;
	}

	case KEXECUTE:
	case KBEGIN:
	case KCOMMIT:
		stmt_error(self, ast, "cannot be used here");
		break;

	case KEOF:
		stmt_error(self, NULL, "unexpected end of statement");
		break;

	default:
	{
		// handle RETURN expr as RETURN SELECT expr
		if (self->is_return)
		{
			stmt_push(self, ast);
			auto select = parse_select_expr(self, NULL);
			self->id  = STMT_SELECT;
			self->ast = &select->ast;
			self->ret = &select->ret;
			break;
		}

		stmt_error(self, ast, "unexpected statement");
		break;
	}
	}

	// mark stmt as sending if it has sending UDF calls
	if (self->udfs_sending)
		self->block->stmts.last_send = self;

	// validate RETURN usage
	if (self->is_return)
	{
		// return stmt
		// return expr

		// ensure stmt is returning
		if (! self->ret)
			stmt_error(self, ast, "RETURN statement must return data");
	}

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
		auto next = lex_next(lex);
		if (next->id == ';')
			continue;

		switch (next->id) {
		case KWITH:
			// [WITH name AS ( cte )[, name AS (...)]]
			parse_with(self, block);
			break;
		case KDECLARE:
		case KNAME:
			// [DECLARE] var type
			// [DECLARE] var type = expr
			// var = expr
			lex_push(lex, next);
			parse_declare_or_assign(self, block);
			break;
		case KEND:
		case KELSE:
		case KELSIF:
			// block end
			lex_push(lex, next);
			return;
		case KEOF:
			return;

		default:
		{
			// stmt
			lex_push(lex, next);
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
		self->explain = true;
		if (lex_if(lex, '('))
		{
			lex_expect(lex, KPROFILE);
			lex_expect(lex, ')');
			self->explain = false;
			self->profile = true;
		}

	} else
	if (lex_if(lex, KPROFILE))
		self->profile = true;

	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// EXECUTE | BEGIN
	if (lex_if(lex, KEXECUTE))
	{
		// EXECUTE function_name(args, ...)
		auto stmt = stmt_allocate(self, lex, block);
		stmt->id = STMT_EXECUTE;
		stmts_add(&block->stmts, stmt);
		parse_execute(stmt);
		lex_if(lex, ';');
	} else
	{
		// [BEGIN]
		auto begin = lex_if(lex, KBEGIN) != NULL;

		// stmt [; stmt]
		parse_block(self, block);

		// [END [;]]
		if (begin)
		{
			lex_expect(lex, KEND);
			lex_if(lex, ';');
		}
	}

	// EOF
	auto ast = lex_next(lex);
	if (ast->id != KEOF)
		lex_error(lex, ast, "unexpected token at the end of transaction");

	// ensure EXPLAIN has a command
	if (unlikely(! block->stmts.count))
		if (self->explain || self->profile)
			lex_error(lex, explain, "EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (block->stmts.count_utility && block->stmts.count > 1)
		lex_error(lex, ast, "multi-statement utility commands are not supported");

	// mark last stmt as return
	auto last = block->stmts.list_tail;
	if (last && !last->is_return)
		last->is_return = true;
}

void
parse_udf(Parser* self, Program* program, Udf* udf)
{
	self->program = program;

	// prepare parser
	auto lex = &self->lex;
	lex_start(&self->lex, &udf->config->text);

	// BEGIN
	lex_expect(lex, KBEGIN);

	// create main namespace and the main block
	auto ns = namespaces_add(&self->nss, NULL, udf->config);

	// precreate arguments as variables
	list_foreach(&udf->config->args.list)
	{
		auto column = list_at(Column, link);
		vars_add(&ns->vars, &column->name, column->type, true);
	}

	auto block = blocks_add(&ns->blocks, NULL, NULL);
	parse_block(self, block);

	// END
	auto end = lex_expect(lex, KEND);

	// ensure udf has no recursion
	if (access_find(&program->access, &udf->config->db, &udf->config->name))
		lex_error(lex, end, "UDF recursion is not supported");

	// ensure main stmt is not utility when using CTE
	if (block->stmts.count_utility > 1)
		lex_error(lex, end, "utility commands are not supported with UDF");

	// automatically add return statement at the end
	if (block->stmts.count > 0)
	{
		// mark last stmt as return
		auto last = block->stmts.list_tail;
		if (last->is_return)
			return;
	}

	// RETURN;
	auto stmt = stmt_allocate(self, lex, block);
	stmt->is_return = true;
	stmt->id = STMT_RETURN;
	stmts_add(&block->stmts, stmt);
}
