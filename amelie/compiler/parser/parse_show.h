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

typedef struct AstShow AstShow;
typedef struct AstSet  AstSet;

struct AstShow
{
	Ast  ast;
	Str  section;
	Str  name;
	Str  schema;
	Str  format;
	bool extended;
};

static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline AstShow*
ast_show_allocate(void)
{
	AstShow* self = ast_allocate(0, sizeof(AstShow));
	return self;
}

void parse_show(Stmt*);
