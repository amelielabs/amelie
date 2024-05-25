
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

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
	def_free(&stmt->def);
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

	case KCHECKPOINT:
		// CHECKPOINT
		stmt->id = STMT_CHECKPOINT;
		parse_checkpoint(stmt);
		break;

	case KCREATE:
	{
		// CREATE USER | SCHEMA | TABLE | VIEW
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_CREATE_USER;
			parse_user_create(stmt);
		} else
		if (lex_if(lex, KSCHEMA))
		{
			stmt->id = STMT_CREATE_SCHEMA;
			parse_schema_create(stmt);
		} else
		if (lex_if(lex, KTABLE))
		{
			stmt->id = STMT_CREATE_TABLE;
			parse_table_create(stmt);
		} else
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_CREATE_VIEW;
			parse_view_create(stmt);
		} else {
			error("CREATE <USER|SCHEMA|TABLE|VIEW> expected");
		}
		break;
	}

	case KDROP:
	{
		// DROP USER | SCHEMA | TABLE | VIEW
		if (lex_if(lex, KUSER))
		{
			stmt->id = STMT_DROP_USER;
			parse_user_drop(stmt);
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
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_DROP_VIEW;
			parse_view_drop(stmt);
		} else {
			error("DROP <USER|SCHEMA|TABLE|VIEW> expected");
		}
		break;
	}

	case KALTER:
	{
		// ALTER USER | SCHEMA | TABLE | VIEW
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
		if (lex_if(lex, KVIEW))
		{
			stmt->id = STMT_ALTER_VIEW;
			parse_view_alter(stmt);
		} else {
			error("ALTER <USER|SCHEMA|TABLE|VIEW> expected");
		}
		break;
	}

	case KINSERT:
	case KREPLACE:
		stmt->id = STMT_INSERT;
		parse_insert(stmt, ast->id == KINSERT);
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

	default:
	{
		if (ast->id == KNAME)
			error("unknown command: <%.*s>", str_size(&ast->string),
			      str_of(&ast->string));
		error("unknown command");
		break;
	}
	}
}

hot void
parse_cte_args(Parser* self, Stmt* cte)
{
	// )
	auto lex = &self->lex;
	if (lex_if(lex, ')'))
		return;

	for (;;)
	{
		// name
		auto name = lex_if(lex, KNAME);
		if (! name)
			error("WITH name (<name> expected");

		// ensure arg does not exists
		auto arg = def_find_column(&cte->def, &name->string);
		if (arg)
			error("<%.*s> view argument redefined", str_size(&name->string),
			      str_of(&name->string));

		// add argument
		arg = column_allocate();
		def_add_column(&cte->def, arg);
		column_set_name(arg, &name->string);

		// ,
		if (! lex_if(lex, ','))
			break;
	}

	// )
	if (! lex_if(lex, ')'))
		error("WITH name (<)> expected");
}

hot void
parse_cte(Parser* self)
{
	auto lex = &self->lex;

	// WITH
	if (! lex_if(lex, KWITH))
		return;

	for (;;)
	{
		// name [(args)] AS ( stmt )[, ...]
		auto cte = stmt_allocate(self->db, &self->lex, self->data, &self->json,
		                         &self->stmt_list);
		stmt_list_add(&self->stmt_list, cte);

		// name
		cte->name = lex_if(lex, KNAME);
		if (! cte->name)
			error("WITH <name> expected");

		// (args)
		if (lex_if(lex, '('))
			parse_cte_args(self, cte);

		// AS
		if (! lex_if(lex, KAS))
			error("WITH name <AS> expected");

		// (
		if (! lex_if(lex, '('))
			error("WITH name AS <(> expected");

		// stmt (cannot be a utility statement)
		parse_stmt(self, cte);
		if (stmt_is_utility(cte))
			error("CTE must DML or Select");

		// )
		if (! lex_if(lex, ')'))
			error("WITH name AS (<)> expected");

		// ,
		if (! lex_if(lex, ','))
			break;
	}
}

hot void
parse(Parser* self, Str* str)
{
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

	// [WITH name AS ( cte )[, name AS (...)]]
	parse_cte(self);

	// stmt [; stmt]
	bool has_utility = false;	
	for (;;)
	{
		// EOF
		if (lex_if(lex, KEOF))
			break;

		// ; | EOF
		lex_if(lex, ';');
		if (lex_if(lex, KEOF))
			break;

		// stmt (last stmt is main)
		self->stmt = stmt_allocate(self->db, &self->lex, self->data, &self->json,
		                           &self->stmt_list);
		stmt_list_add(&self->stmt_list, self->stmt);
		parse_stmt(self, self->stmt);
		if (stmt_is_utility(self->stmt))
			has_utility = true;
	}

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !self->stmt_list.list_count))
		error("EXPLAIN without command");

	// ensure main stmt is not utility when using CTE
	if (has_utility && self->stmt_list.list_count > 1)
		error("CTE or multi-statement utility commands are not supported");
}
