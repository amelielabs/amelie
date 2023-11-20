#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Stmt Stmt;

typedef enum
{
	STMT_UNDEF,
	STMT_BEGIN,
	STMT_COMMIT,
	STMT_SHOW,
	STMT_SET,
	STMT_CREATE_USER,
	STMT_CREATE_SCHEMA,
	STMT_CREATE_TABLE,
	STMT_CREATE_VIEW,
	STMT_DROP_USER,
	STMT_DROP_SCHEMA,
	STMT_DROP_TABLE,
	STMT_DROP_VIEW,
	STMT_ALTER_USER,
	STMT_ALTER_SCHEMA,
	STMT_ALTER_TABLE,
	STMT_ALTER_VIEW,
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
	TargetList target_list;
	Lex*       lex;
	Db*        db;
	List       link;
};

static inline Stmt*
stmt_allocate(int order, Db* db, Lex* lex)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id    = STMT_UNDEF;
	self->order = order;
	self->ast   = NULL;
	self->lex   = lex;
	self->db    = db;
	target_list_init(&self->target_list);
	list_init(&self->link);
	return self;
}

always_inline static inline Ast*
stmt_next_shadow(Stmt* self)
{
	lex_set_keywords(self->lex, false);
	auto ast = lex_next(self->lex);
	lex_set_keywords(self->lex, true);
	return ast;
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
