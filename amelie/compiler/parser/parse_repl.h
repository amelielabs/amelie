#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstReplCtl     AstReplCtl;
typedef struct AstReplPromote AstReplPromote;

struct AstReplCtl
{
	Ast  ast;
	bool start;
};

struct AstReplPromote
{
	Ast  ast;
	Ast* id;
};

static inline AstReplCtl*
ast_repl_ctl_of(Ast* ast)
{
	return (AstReplCtl*)ast;
}

static inline AstReplCtl*
ast_repl_ctl_allocate(bool start)
{
	AstReplCtl* self;
	self = ast_allocate(0, sizeof(AstReplCtl));
	self->start = start;
	return self;
}

static inline AstReplPromote*
ast_repl_promote_of(Ast* ast)
{
	return (AstReplPromote*)ast;
}

static inline AstReplPromote*
ast_repl_promote_allocate(void)
{
	AstReplPromote* self;
	self = ast_allocate(0, sizeof(AstReplPromote));
	self->id = NULL;
	return self;
}

void parse_repl_start(Stmt*);
void parse_repl_stop(Stmt*);
void parse_repl_promote(Stmt*);
