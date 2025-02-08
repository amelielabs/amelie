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

typedef struct AstWorkerCreate AstWorkerCreate;
typedef struct AstWorkerDrop   AstWorkerDrop;

struct AstWorkerCreate
{
	Ast  ast;
	bool if_not_exists;
	Ast* id;
};

struct AstWorkerDrop
{
	Ast  ast;
	bool if_exists;
	Ast* id;
};

static inline AstWorkerCreate*
ast_worker_create_of(Ast* ast)
{
	return (AstWorkerCreate*)ast;
}

static inline AstWorkerCreate*
ast_worker_create_allocate(void)
{
	AstWorkerCreate* self;
	self = ast_allocate(0, sizeof(AstWorkerCreate));
	self->if_not_exists = false;
	return self;
}

static inline AstWorkerDrop*
ast_worker_drop_of(Ast* ast)
{
	return (AstWorkerDrop*)ast;
}

static inline AstWorkerDrop*
ast_worker_drop_allocate(void)
{
	AstWorkerDrop* self;
	self = ast_allocate(0, sizeof(AstWorkerDrop));
	self->if_exists = false;
	self->id        = NULL;
	return self;
}

void parse_worker_create(Stmt*);
void parse_worker_drop(Stmt*);
