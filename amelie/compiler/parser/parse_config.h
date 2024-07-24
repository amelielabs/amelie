#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct AstShow AstShow;
typedef struct AstSet  AstSet;

struct AstShow
{
	Ast  ast;
	Ast* expr;
};

struct AstSet
{
	Ast  ast;
	Ast* name;
	Ast* value;
};

static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline AstShow*
ast_show_allocate(Ast* expr)
{
	AstShow* self = ast_allocate(0, sizeof(AstShow));
	self->expr = expr;
	return self;
}

static inline AstSet*
ast_set_of(Ast* ast)
{
	return (AstSet*)ast;
}

static inline AstSet*
ast_set_allocate(Ast* name, Ast* value)
{
	AstSet* self = ast_allocate(0, sizeof(AstSet));
	self->name  = name;
	self->value = value;
	return self;
}

static inline void
parse_show(Stmt* self)
{
	// SHOW name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME)
		error("SHOW <name> expected");
	auto stmt = ast_show_allocate(name);
	self->ast = &stmt->ast;
}

static inline void
parse_set(Stmt* self)
{
	// SET name TO INT|STRING
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME)
		error("SET <name> expected");
	// TO
	if (! stmt_if(self, KTO))
		error("SET name <TO> expected");
	// value
	auto value = stmt_next(self);
	if (value->id != KINT && value->id != KSTRING)
		error("SET name TO <value> expected string or int");
	auto stmt = ast_set_allocate(name, value);
	self->ast = &stmt->ast;
}
