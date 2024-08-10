#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Parser Parser;

enum
{
	EXPLAIN_NONE    = 0,
	EXPLAIN         = 1,
	EXPLAIN_PROFILE = 2
};

struct Parser
{
	int          explain;
	StmtList     stmt_list;
	Stmt*        stmt;
	CodeData*    data;
	Json         json;
	Lex          lex;
	FunctionMgr* function_mgr;
	Local*       local;
	Db*          db;
};

void parser_init(Parser*, Db*, FunctionMgr*, CodeData*);
void parser_reset(Parser*);
void parser_free(Parser*);

static inline bool
parser_is_profile(Parser* self)
{
	return self->explain == (EXPLAIN|EXPLAIN_PROFILE);
}

static inline bool
parser_is_shared_table_dml(Parser* self)
{
	list_foreach(&self->stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		if ((stmt->id == STMT_INSERT ||
		     stmt->id == STMT_UPDATE ||
		     stmt->id == STMT_DELETE) &&
		    target_list_has(&stmt->target_list, TARGET_TABLE_SHARED))
			return true;
	}
	return false;
}
