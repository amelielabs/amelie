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

typedef struct AstReplCtl       AstReplCtl;
typedef struct AstReplSubscribe AstReplSubscribe;

struct AstReplCtl
{
	Ast  ast;
	bool start;
};

struct AstReplSubscribe
{
	Ast  ast;
	Ast* id;
};

static inline AstReplCtl*
ast_repl_ctl_of(Ast* ast)
{
	return (AstReplCtl*)ast;
}

static inline AstReplCtl*
ast_repl_ctl_allocate(bool start)
{
	AstReplCtl* self;
	self = ast_allocate(0, sizeof(AstReplCtl));
	self->start = start;
	return self;
}

static inline AstReplSubscribe*
ast_repl_subscribe_of(Ast* ast)
{
	return (AstReplSubscribe*)ast;
}

static inline AstReplSubscribe*
ast_repl_subscribe_allocate(void)
{
	AstReplSubscribe* self;
	self = ast_allocate(0, sizeof(AstReplSubscribe));
	self->id = NULL;
	return self;
}

void parse_repl_start(Stmt*);
void parse_repl_stop(Stmt*);
void parse_repl_subscribe(Stmt*);
void parse_repl_unsubscribe(Stmt*);
