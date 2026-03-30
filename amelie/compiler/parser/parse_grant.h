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

typedef struct AstGrant AstGrant;

struct AstGrant
{
	Ast      ast;
	bool     grant;
	uint32_t perms;
	Str      rel_user;
	Str      rel;
	Str      to;
};

static inline AstGrant*
ast_grant_of(Ast* ast)
{
	return (AstGrant*)ast;
}

static inline AstGrant*
ast_grant_allocate(void)
{
	AstGrant* self = ast_allocate(0, sizeof(AstGrant));
	return self;
}

void parse_grant(Stmt*, bool);
