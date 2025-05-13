#pragma once

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

typedef struct Stmt   Stmt;
typedef struct Stmts  Stmts;
typedef struct Parser Parser;

typedef enum
{
	STMT_UNDEF,
	STMT_SHOW,
	STMT_SET,
	STMT_START_REPL,
	STMT_STOP_REPL,
	STMT_SUBSCRIBE,
	STMT_UNSUBSCRIBE,
	STMT_CHECKPOINT,
	STMT_CREATE_USER,
	STMT_CREATE_TOKEN,
	STMT_CREATE_REPLICA,
	STMT_CREATE_SCHEMA,
	STMT_CREATE_TABLE,
	STMT_CREATE_INDEX,
	STMT_CREATE_PROCEDURE,
	STMT_DROP_USER,
	STMT_DROP_REPLICA,
	STMT_DROP_SCHEMA,
	STMT_DROP_TABLE,
	STMT_DROP_INDEX,
	STMT_DROP_PROCEDURE,
	STMT_ALTER_USER,
	STMT_ALTER_SCHEMA,
	STMT_ALTER_TABLE,
	STMT_ALTER_INDEX,
	STMT_ALTER_PROCEDURE,
	STMT_RETURN,
	STMT_EXECUTE,
	STMT_CALL,
	STMT_TRUNCATE,
	STMT_INSERT,
	STMT_UPDATE,
	STMT_DELETE,
	STMT_SELECT,
	STMT_WATCH
} StmtId;

struct Stmt
{
	Lex*    lex;
	StmtId  id;
	Ast*    ast;
	int     order;
	int     order_targets;
	bool    ret;
	Cte*    cte;
	int     r;
	Var*    assign;
	AstList select_list;
	Scope*  scope;
	Parser* parser;
	Stmt*   next;
	Stmt*   prev;
};

static inline Stmt*
stmt_allocate(Parser* parser, Lex* lex, Scope* scope)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id            = STMT_UNDEF;
	self->ast           = NULL;
	self->order         = 0;
	self->order_targets = 0;
	self->ret           = false;
	self->cte           = NULL;
	self->r             = -1;
	self->assign        = NULL;
	self->next          = NULL;
	self->prev          = NULL;
	self->lex           = lex;
	self->scope         = scope;
	self->parser        = parser;
	ast_list_init(&self->select_list);
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

static inline void
stmt_error(Stmt* self, Ast* ast, const char* fmt, ...)
{
	va_list args;
	char msg[256];
	va_start(args, fmt);
	vsnprintf(msg, sizeof(msg), fmt, args);
	va_end(args);
	if (! ast)
		ast = lex_next(self->lex);
	lex_error(self->lex, ast, msg);
}

always_inline static inline Ast*
stmt_expect(Stmt* self, int id)
{
	auto ast = lex_next(self->lex);
	if (ast->id == id)
		return ast;
	lex_error_expect(self->lex, ast, id);
	return NULL;
}

always_inline static inline void
stmt_push(Stmt* self, Ast* ast)
{
	lex_push(self->lex, ast);
}

static inline bool
stmt_is_utility(Stmt* self)
{
	switch (self->id) {
	case STMT_SHOW:
	case STMT_SET:
	case STMT_START_REPL:
	case STMT_STOP_REPL:
	case STMT_SUBSCRIBE:
	case STMT_UNSUBSCRIBE:
	case STMT_CHECKPOINT:
	case STMT_CREATE_USER:
	case STMT_CREATE_TOKEN:
	case STMT_CREATE_REPLICA:
	case STMT_CREATE_SCHEMA:
	case STMT_CREATE_TABLE:
	case STMT_CREATE_INDEX:
	case STMT_CREATE_PROCEDURE:
	case STMT_DROP_USER:
	case STMT_DROP_REPLICA:
	case STMT_DROP_SCHEMA:
	case STMT_DROP_TABLE:
	case STMT_DROP_INDEX:
	case STMT_DROP_PROCEDURE:
	case STMT_ALTER_USER:
	case STMT_ALTER_SCHEMA:
	case STMT_ALTER_TABLE:
	case STMT_ALTER_INDEX:
	case STMT_ALTER_PROCEDURE:
	case STMT_TRUNCATE:
		return true;
	default:
		break;
	}
	return false;
}
