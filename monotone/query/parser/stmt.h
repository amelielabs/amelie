#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Stmt Stmt;

typedef enum
{
	STMT_SHOW,
	STMT_SET,
	STMT_CREATE_USER,
	STMT_CREATE_TABLE,
	STMT_DROP_USER,
	STMT_DROP_TABLE,
	STMT_ALTER_USER,
	STMT_ALTER_TABLE,
	STMT_INSERT,
	STMT_UPDATE,
	STMT_DELETE,
	STMT_SELECT
} StmtId;

struct Stmt
{
	StmtId     id;
	int        order;
	Ast*       ast;
	AstList    aggr_list;
	TargetList target_list;
	Lex*       lex;
	Db*        db;
	List       link;
};

static inline Stmt*
stmt_allocate(StmtId id, int order, Db* db, Lex* lex)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id    = id;
	self->order = order;
	self->ast   = NULL;
	self->lex   = lex;
	self->db    = db;
	ast_list_init(&self->aggr_list);
	target_list_init(&self->target_list);
	list_init(&self->link);
	return self;
}

always_inline static inline Ast*
stmt_next(Stmt* self)
{
	return lex_next(self->lex);
}

always_inline static inline Ast*
stmt_if(Stmt* self, int id)
{
	return lex_if(self->lex, id);
}

always_inline static inline void
stmt_push(Stmt* self, Ast* ast)
{
	lex_push(self->lex, ast);
}
