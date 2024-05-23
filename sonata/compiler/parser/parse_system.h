#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstCheckpoint AstCheckpoint;

struct AstCheckpoint
{
	Ast  ast;
	Ast* workers;
};

static inline AstCheckpoint*
ast_checkpoint_of(Ast* ast)
{
	return (AstCheckpoint*)ast;
}

static inline AstCheckpoint*
ast_checkpoint_allocate(void)
{
	AstCheckpoint* self = ast_allocate(0, sizeof(AstCheckpoint));
	self->workers = NULL;
	return self;
}

void parse_checkpoint(Stmt*);
