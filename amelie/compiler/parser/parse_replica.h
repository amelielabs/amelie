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

typedef struct AstReplicaCreate AstReplicaCreate;
typedef struct AstReplicaDrop   AstReplicaDrop;

struct AstReplicaCreate
{
	Ast    ast;
	bool   if_not_exists;
	Ast*   id;
	Remote remote;
};

struct AstReplicaDrop
{
	Ast  ast;
	bool if_exists;
	Ast* id;
};

static inline AstReplicaCreate*
ast_replica_create_of(Ast* ast)
{
	return (AstReplicaCreate*)ast;
}

static inline AstReplicaCreate*
ast_replica_create_allocate(void)
{
	AstReplicaCreate* self;
	self = ast_allocate(0, sizeof(AstReplicaCreate));
	self->if_not_exists = false;
	remote_init(&self->remote);
	return self;
}

static inline AstReplicaDrop*
ast_replica_drop_of(Ast* ast)
{
	return (AstReplicaDrop*)ast;
}

static inline AstReplicaDrop*
ast_replica_drop_allocate(void)
{
	AstReplicaDrop* self;
	self = ast_allocate(0, sizeof(AstReplicaDrop));
	self->if_exists = false;
	self->id        = NULL;
	return self;
}

void parse_replica_create(Stmt*);
void parse_replica_drop(Stmt*);
