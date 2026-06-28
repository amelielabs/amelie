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

typedef struct AstMatching AstMatching;

struct AstMatching
{
	Ast       ast;
	Returning ret;
	Column*   column;
	Table*    table;
	Ast*      expr_to;
	Ast*      expr_top;
	From      from;
};

static inline AstMatching*
ast_matching_of(Ast* ast)
{
	return (AstMatching*)ast;
}

static inline AstMatching*
ast_matching_allocate(From* outer, Block* block)
{
	AstMatching* self;
	self = ast_allocate(KMATCHING, sizeof(AstMatching));
	from_init(&self->from, block);
	from_set_outer(&self->from, outer);
	returning_init(&self->ret);
	return self;
}

AstMatching* parse_matching(Stmt*, From*, Table*);
