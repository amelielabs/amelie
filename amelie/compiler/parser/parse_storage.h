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

typedef struct AstStorageCreate AstStorageCreate;
typedef struct AstStorageDrop   AstStorageDrop;
typedef struct AstStorageAlter  AstStorageAlter;

struct AstStorageCreate
{
	Ast            ast;
	bool           if_not_exists;
	StorageConfig* config;
};

struct AstStorageDrop
{
	Ast  ast;
	bool if_exists;
	Ast* name;
};

struct AstStorageAlter
{
	Ast  ast;
	bool if_exists;
	Ast* name;
	Ast* name_new;
};

static inline AstStorageCreate*
ast_storage_create_of(Ast* ast)
{
	return (AstStorageCreate*)ast;
}

static inline AstStorageCreate*
ast_storage_create_allocate(void)
{
	AstStorageCreate* self;
	self = ast_allocate(0, sizeof(AstStorageCreate));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

static inline AstStorageDrop*
ast_storage_drop_of(Ast* ast)
{
	return (AstStorageDrop*)ast;
}

static inline AstStorageDrop*
ast_storage_drop_allocate(void)
{
	AstStorageDrop* self;
	self = ast_allocate(0, sizeof(AstStorageDrop));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

static inline AstStorageAlter*
ast_storage_alter_of(Ast* ast)
{
	return (AstStorageAlter*)ast;
}

static inline AstStorageAlter*
ast_storage_alter_allocate(void)
{
	AstStorageAlter* self;
	self = ast_allocate(0, sizeof(AstStorageAlter));
	self->if_exists = false;
	self->name      = NULL;
	self->name_new  = NULL;
	return self;
}

void    parse_storage_create(Stmt*);
void    parse_storage_drop(Stmt*);
void    parse_storage_alter(Stmt*);
Volume* parse_volume(Stmt*);
void    parse_volumes(Stmt*, VolumeMgr*);
