
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static inline Stmt*
parser_add(Parser* self)
{
	auto stmt = stmt_allocate(self->stmts_count, self->db, &self->lex);
	list_append(&self->stmts, &stmt->link);
	self->stmts_count++;
	return stmt;
}

static inline void
parse_show(Stmt* self)
{
	// SHOW name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("SHOW <name> expected");
	auto stmt = ast_show_allocate(name);
	self->ast = &stmt->ast;
}

static inline void
parse_set(Stmt* self)
{
	// SET name TO INT|STRING
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("SET <name> expected");
	// TO
	if (! stmt_if(self, KTO))
		error("SET name <TO> expected");
	// value
	auto value = stmt_next(self);
	if (value->id != KINT && value->id != KSTRING)
		error("SET name TO <value> expected string or int");
	auto stmt = ast_set_allocate(name, value);
	self->ast = &stmt->ast;
}

hot static inline void
parse_stmt(Parser* self)
{
	auto lex  = &self->lex;
	auto stmt = parser_add(self);

	auto ast = lex_next(lex);
	if (ast->id == KEOF)
		return;

	switch (ast->id) {
	case KBEGIN:
		error("BEGIN: already in transaction");
		break;

	case KABORT:
		// ABORT
		stmt->id = STMT_ABORT;
		break;

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
		error("unknown command");
		break;
	}

	// ; | EOF
	ast = lex_next(lex);
	if (likely(ast->id == ';' || ast->id == KEOF))
	{
		lex_push(lex, ast);
		return;
	}

	error("unexpected clause at the end of command");
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

	// [BEGIN]
	bool begin  = false;
	bool commit = false;
	if (lex_if(lex, KBEGIN))
		begin = true;

	// statement [; statement]
	for (;;)
	{
		// ;
		if (lex_if(lex, ';'))
			continue;

		// EOF
		if (lex_if(lex, KEOF))
			break;

		// [COMMIT]
		if (lex_if(lex, KCOMMIT))
		{
			if (! begin)
				error("COMMIT without BEGIN");
			commit = true;

			// [;]
			lex_if(lex, ';');

			// EOF
			if (unlikely(! lex_if(lex, KEOF)))
				error("unexpected command after COMMIT");

			break;
		}

		// statement
		parse_stmt(self);
	}

	// ensure we did not forget about COMMIT
	if (unlikely(begin && !commit))
		error("COMMIT is missing at the end of the multi-statement transaction");

	// ensure EXPLAIN has command
	if (unlikely(self->explain && !self->stmts_count))
		error("EXPLAIN without command");

	// ensure we are can execute transactional commands
	if (parser_has_utility(self) && (begin || self->stmts_count != 1))
		error("DDL and utility operations are not transactional");
}
