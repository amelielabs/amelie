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

typedef struct AstReplFollow AstReplFollow;

struct AstReplFollow
{
	Ast  ast;
	Ast* id;
};

static inline AstReplFollow*
ast_repl_follow_of(Ast* ast)
{
	return (AstReplFollow*)ast;
}

static inline AstReplFollow*
ast_repl_follow_allocate(void)
{
	AstReplFollow* self;
	self = ast_allocate(0, sizeof(AstReplFollow));
	self->id = NULL;
	return self;
}

void parse_repl_follow(Stmt*);
void parse_repl_unfollow(Stmt*);
