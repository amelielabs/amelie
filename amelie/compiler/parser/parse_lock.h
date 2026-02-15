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

typedef struct AstLockCreate AstLockCreate;
typedef struct AstLockDrop   AstLockDrop;

struct AstLockCreate
{
	Ast  ast;
	bool if_not_exists;
	Str  name;
	Str  name_rel;
	Str  name_lock;
};

struct AstLockDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

static inline AstLockCreate*
ast_lock_create_of(Ast* ast)
{
	return (AstLockCreate*)ast;
}

static inline AstLockCreate*
ast_lock_create_allocate(void)
{
	AstLockCreate* self;
	self = ast_allocate(0, sizeof(AstLockCreate));
	return self;
}

static inline AstLockDrop*
ast_lock_drop_of(Ast* ast)
{
	return (AstLockDrop*)ast;
}

static inline AstLockDrop*
ast_lock_drop_allocate(void)
{
	AstLockDrop* self;
	self = ast_allocate(0, sizeof(AstLockDrop));
	return self;
}

void parse_lock_create(Stmt*);
void parse_lock_drop(Stmt*);
