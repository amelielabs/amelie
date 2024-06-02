#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct AstNodeCreate AstNodeCreate;
typedef struct AstNodeDrop   AstNodeDrop;
typedef struct AstNodeAlter  AstNodeAlter;

struct AstNodeCreate
{
	Ast  ast;
	bool if_not_exists;
	Ast* id;
	Ast* uri;
	Ast* type;
};

struct AstNodeDrop
{
	Ast  ast;
	bool if_exists;
	Ast* id;
};

struct AstNodeAlter
{
	Ast  ast;
	bool if_exists;
	Ast* id;
	Ast* uri;
};

static inline AstNodeCreate*
ast_node_create_of(Ast* ast)
{
	return (AstNodeCreate*)ast;
}

static inline AstNodeCreate*
ast_node_create_allocate(void)
{
	AstNodeCreate* self;
	self = ast_allocate(0, sizeof(AstNodeCreate));
	self->if_not_exists = false;
	self->id            = NULL;
	self->uri           = NULL;
	self->type          = NULL;
	return self;
}

static inline AstNodeDrop*
ast_node_drop_of(Ast* ast)
{
	return (AstNodeDrop*)ast;
}

static inline AstNodeDrop*
ast_node_drop_allocate(void)
{
	AstNodeDrop* self;
	self = ast_allocate(0, sizeof(AstNodeDrop));
	self->if_exists = false;
	self->id        = NULL;
	return self;
}

static inline AstNodeAlter*
ast_node_alter_of(Ast* ast)
{
	return (AstNodeAlter*)ast;
}

static inline AstNodeAlter*
ast_node_alter_allocate(void)
{
	AstNodeAlter* self;
	self = ast_allocate(0, sizeof(AstNodeAlter));
	self->if_exists = false;
	self->id        = NULL;
	self->uri       = NULL;
	return self;
}

void parse_node_create(Stmt*);
void parse_node_drop(Stmt*);
void parse_node_alter(Stmt*);
