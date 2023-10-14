
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static inline void
parser_validate(Parser* self, bool transactional)
{
	if (unlikely(self->complete))
		error("transaction is complete");

	if (! transactional)
		if (unlikely(self->in_transaction || self->stmts_count > 0))
			error("operation is not transactional");
}

static inline Stmt*
parser_add(Parser* self, StmtId id)
{
	auto stmt = stmt_allocate(id, self->db, &self->lex);
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

	bool eof = false;
	while (! eof)
	{
		// todo: explain

		auto ast = lex_next(lex);
		switch (ast->id) {
		case KBEGIN:
			// BEGIN
			parser_validate(self, true);
			if (self->in_transaction)
				error("already in transaction");
			self->in_transaction = true;
			break;

		case KCOMMIT:
			// COMMIT
			parser_validate(self, true);
			if (! self->in_transaction)
				error("not in transaction");
			self->complete = true;
			break;

		case KSHOW:
		{
			// SHOW name
			parser_validate(self, false);
			auto stmt = parser_add(self, STMT_SHOW);
			parse_show(stmt);
			break;
		}

		case KSET:
		{
			// SET name TO INT|STRING
			parser_validate(self, false);
			auto stmt = parser_add(self, STMT_SET);
			parse_set(stmt);
			break;
		}

		case KCREATE:
		{
			// CREATE TABLE | USER
			parser_validate(self, false);
			if (lex_if(lex, KUSER))
			{
				auto stmt = parser_add(self, STMT_CREATE_USER);
				parse_user_create(stmt);
			} else
			if (lex_if(lex, KTABLE))
			{
				auto stmt = parser_add(self, STMT_CREATE_TABLE);
				parse_table_create(stmt);
			} else
				error("CREATE <USER|TABLE> expected");
			break;
		}

		case KDROP:
		{
			// DROP TABLE | USER
			parser_validate(self, false);
			if (lex_if(lex, KUSER))
			{
				auto stmt = parser_add(self, STMT_DROP_USER);
				parse_user_drop(stmt);
			} else
			if (lex_if(lex, KTABLE))
			{
				auto stmt = parser_add(self, STMT_DROP_TABLE);
				parse_table_drop(stmt);
			}
			else
				error("DROP <USER|TABLE> expected");
			break;
		}

		case KALTER:
		{
			// ALTER TABLE | USER
			parser_validate(self, false);
			if (lex_if(lex, KUSER))
			{
				auto stmt = parser_add(self, STMT_ALTER_USER);
				parse_user_alter(stmt);
			} else
			if (lex_if(lex, KTABLE))
			{
				auto stmt = parser_add(self, STMT_ALTER_TABLE);
				parse_table_alter(stmt);
			} else
				error("ALTER <USER|TABLE> expected");
			break;
		}

		case KINSERT:
		case KREPLACE:
		{
			auto stmt = parser_add(self, STMT_INSERT);
			parse_insert(stmt, ast->id == KINSERT);
			break;
		}
		case KUPDATE:
		{
			auto stmt = parser_add(self, STMT_UPDATE);
			parse_update(stmt);
			break;
		}
		case KDELETE:
		{
			auto stmt = parser_add(self, STMT_DELETE);
			parse_delete(stmt);
			break;
		}
		case KSELECT:
		{
			auto stmt   = parser_add(self, STMT_SELECT);
			auto select = parse_select(stmt);
			stmt->ast = &select->ast;
			break;
		}
		case KEOF:
			eof = true;
			break;
		default:
			break;
		}

		// ;
		lex_if(&self->lex, ';');
	}
}
