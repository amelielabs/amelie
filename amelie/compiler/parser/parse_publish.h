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

typedef struct AstPublish AstPublish;

struct AstPublish
{
	Ast      ast;
	Channel* channel;
	Ast*     expr;
	From     from;
};

static inline AstPublish*
ast_publish_of(Ast* ast)
{
	return (AstPublish*)ast;
}

static inline AstPublish*
ast_publish_allocate(Block* block)
{
	AstPublish* self;
	self = ast_allocate(0, sizeof(AstPublish));
	from_init(&self->from, block);
	return self;
}

void parse_publish(Stmt*);
