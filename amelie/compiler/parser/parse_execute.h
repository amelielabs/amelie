#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstExecute AstExecute;

struct AstExecute
{
	Ast  ast;
	Udf* udf;
};

static inline AstExecute*
ast_execute_of(Ast* ast)
{
	return (AstExecute*)ast;
}

static inline AstExecute*
ast_execute_allocate(void)
{
	AstExecute* self;
	self = ast_allocate(KEXECUTE, sizeof(AstExecute));
	self->udf = NULL;
	return self;
}

void parse_execute(Stmt*);
