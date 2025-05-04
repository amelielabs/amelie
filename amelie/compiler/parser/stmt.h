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

typedef struct Stmt  Stmt;
typedef struct Stmts Stmts;
typedef struct Scope Scope;

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
	STMT_CREATE_FUNCTION,
	STMT_DROP_USER,
	STMT_DROP_REPLICA,
	STMT_DROP_SCHEMA,
	STMT_DROP_TABLE,
	STMT_DROP_INDEX,
	STMT_DROP_FUNCTION,
	STMT_ALTER_USER,
	STMT_ALTER_SCHEMA,
	STMT_ALTER_TABLE,
	STMT_ALTER_INDEX,
	STMT_ALTER_FUNCTION,
	STMT_TRUNCATE,
	STMT_INSERT,
	STMT_UPDATE,
	STMT_DELETE,
	STMT_SELECT,
	STMT_WATCH
} StmtId;

struct Stmt
{
	StmtId  id;
	Ast*    ast;
	int     order;
	int     order_targets;
	bool    ret;
	Cte*    cte;
	Var*    assign;
	int     r;
	AstList select_list;
	Scope*  scope;
	Stmt*   next;
	Stmt*   prev;
};

struct Stmts
{
	Stmt* list;
	Stmt* list_tail;
	int   count;
	int   count_utility;
};

static inline Stmt*
stmt_allocate(Scope* scope)
{
	Stmt* self = palloc(sizeof(Stmt));
	self->id            = STMT_UNDEF;
	self->ast           = NULL;
	self->order         = 0;
	self->order_targets = 0;
	self->ret           = false;
	self->cte           = NULL;
	self->assign        = NULL;
	self->r             = -1;
	self->scope         = scope;
	self->next          = NULL;
	self->prev          = NULL;
	ast_list_init(&self->select_list);
	return self;
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
	case STMT_CREATE_FUNCTION:
	case STMT_DROP_USER:
	case STMT_DROP_REPLICA:
	case STMT_DROP_SCHEMA:
	case STMT_DROP_TABLE:
	case STMT_DROP_INDEX:
	case STMT_DROP_FUNCTION:
	case STMT_ALTER_USER:
	case STMT_ALTER_SCHEMA:
	case STMT_ALTER_TABLE:
	case STMT_ALTER_INDEX:
	case STMT_ALTER_FUNCTION:
	case STMT_TRUNCATE:
		return true;
	default:
		break;
	}
	return false;
}

static inline void
stmts_init(Stmts* self)
{
	self->list          = NULL;
	self->list_tail     = NULL;
	self->count         = 0;
	self->count_utility = 0;
}

static inline void
stmts_add(Stmts* self, Stmt* stmt)
{
	if (self->list == NULL)
		self->list = stmt;
	else
		self->list_tail->next = stmt;
	stmt->prev = self->list_tail;
	self->list_tail = stmt;
	self->count++;
	if (stmt_is_utility(stmt))
		self->count_utility++;
}

static inline void
stmts_insert(Stmts* self, Stmt* before, Stmt* stmt)
{
	if (before->prev == NULL)
		self->list = stmt;
	else
		before->prev->next = stmt;
	stmt->prev = before->prev;
	stmt->next = before;
	before->prev = stmt;
	self->count++;
}

static inline void
stmts_order(Stmts* self)
{
	int order = 0;
	for (auto stmt = self->list; stmt; stmt = stmt->next)
		stmt->order = order++;
}
