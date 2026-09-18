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

typedef struct AstApiCreate AstApiCreate;
typedef struct AstApiDrop   AstApiDrop;

struct AstApiCreate
{
	Ast  ast;
	Api* config;
};

struct AstApiDrop
{
	Ast ast;
	Str uri;
};

static inline AstApiCreate*
ast_api_create_of(Ast* ast)
{
	return (AstApiCreate*)ast;
}

static inline AstApiCreate*
ast_api_create_allocate(void)
{
	AstApiCreate* self;
	self = ast_allocate(0, sizeof(AstApiCreate));
	return self;
}

static inline AstApiDrop*
ast_api_drop_of(Ast* ast)
{
	return (AstApiDrop*)ast;
}

static inline AstApiDrop*
ast_api_drop_allocate(void)
{
	AstApiDrop* self;
	self = ast_allocate(0, sizeof(AstApiDrop));
	return self;
}

void parse_api_create(Stmt*);
void parse_api_create_inline(Stmt*, Api*);
void parse_api_drop(Stmt*);
