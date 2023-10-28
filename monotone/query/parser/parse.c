
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static inline void
parser_validate(Parser* self, bool transactional)
{
	if (unlikely(self->complete))
		error("transaction is complete");

	if (! transactional)
		if (unlikely(self->in_transaction || self->stmts_count > 1))
			error("operation is not transactional");
}

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

	// statements
	for (;;)
	{
		auto ast = lex_next(lex);
		if (ast->id == KEOF)
			break;

		auto stmt = parser_add(self);
		switch (ast->id) {
		case KBEGIN:
			// BEGIN
			stmt->id = STMT_BEGIN;
			parser_validate(self, true);
			if (self->in_transaction)
				error("already in transaction");
			self->in_transaction = true;
			break;

		case KCOMMIT:
			// COMMIT
			stmt->id = STMT_COMMIT;
			parser_validate(self, true);
			if (! self->in_transaction)
				error("not in transaction");
			self->complete = true;
			break;

		case KSHOW:
			// SHOW name
			stmt->id = STMT_SHOW;
			parser_validate(self, false);
			parse_show(stmt);
			break;

		case KSET:
			// SET name TO INT | STRING
			stmt->id = STMT_SET;
			parser_validate(self, false);
			parse_set(stmt);
			break;

		case KCREATE:
		{
			// CREATE TABLE | SCHEMA | USER
			parser_validate(self, false);
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
				error("CREATE <USER|SCHEMA|TABLE> expected");
			break;
		}

		case KDROP:
		{
			// DROP TABLE | SCHEMA | USER
			parser_validate(self, false);
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
			}
			else
				error("DROP <USER|SCHEMA|TABLE> expected");
			break;
		}

		case KALTER:
		{
			// ALTER TABLE | SCHEMA | USER
			parser_validate(self, false);
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
				error("ALTER <USER|SCHEMA|TABLE> expected");
			break;
		}

		case KINSERT:
		case KREPLACE:
			stmt->id = STMT_INSERT;
			parser_validate(self, true);
			parse_insert(stmt, ast->id == KINSERT);
			break;

		case KUPDATE:
			stmt->id = STMT_UPDATE;
			parser_validate(self, true);
			parse_update(stmt);
			break;

		case KDELETE:
			stmt->id = STMT_DELETE;
			parser_validate(self, true);
			parse_delete(stmt);
			break;

		case KSELECT:
		{
			stmt->id = STMT_SELECT;
			parser_validate(self, true);
			auto select = parse_select(stmt);
			stmt->ast = &select->ast;
			break;
		}

		default:
			error("unknown command");
			break;
		}

		// EOF
		if (lex_if(&self->lex, KEOF))
			break;

		// ;
		if (lex_if(&self->lex, ';'))
			continue;

		error("unexpected clause at the end of command");
	}
}
