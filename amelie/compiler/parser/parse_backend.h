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

typedef struct AstBackendCreate AstBackendCreate;
typedef struct AstBackendDrop   AstBackendDrop;

struct AstBackendCreate
{
	Ast  ast;
	bool if_not_exists;
	Ast* id;
};

struct AstBackendDrop
{
	Ast  ast;
	bool if_exists;
	Ast* id;
};

static inline AstBackendCreate*
ast_backend_create_of(Ast* ast)
{
	return (AstBackendCreate*)ast;
}

static inline AstBackendCreate*
ast_backend_create_allocate(void)
{
	AstBackendCreate* self;
	self = ast_allocate(0, sizeof(AstBackendCreate));
	self->if_not_exists = false;
	return self;
}

static inline AstBackendDrop*
ast_backend_drop_of(Ast* ast)
{
	return (AstBackendDrop*)ast;
}

static inline AstBackendDrop*
ast_backend_drop_allocate(void)
{
	AstBackendDrop* self;
	self = ast_allocate(0, sizeof(AstBackendDrop));
	self->if_exists = false;
	self->id        = NULL;
	return self;
}

void parse_backend_create(Stmt*);
void parse_backend_drop(Stmt*);
