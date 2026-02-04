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

typedef struct AstTierCreate AstTierCreate;
typedef struct AstTierDrop   AstTierDrop;
typedef struct AstTierAlter  AstTierAlter;

struct AstTierCreate
{
	Ast   ast;
	bool  if_not_exists;
	Str   table_name;
	Tier* tier;
};

struct AstTierDrop
{
	Ast  ast;
	bool if_exists;
	Str  table_name;
	Str  name;
};

enum
{
	TIER_ALTER_RENAME,
	TIER_ALTER_STORAGE_ADD,
	TIER_ALTER_STORAGE_DROP,
	TIER_ALTER_STORAGE_PAUSE,
	TIER_ALTER_STORAGE_RESUME,
	TIER_ALTER_SET
};

struct AstTierAlter
{
	Ast   ast;
	bool  if_exists;
	bool  if_exists_storage;
	bool  if_not_exists_storage;
	int   type;
	Str   table_name;
	Str   name;
	Str   name_new;
	Str   name_storage;
	Tier* set;
	int   set_mask;
};

static inline AstTierCreate*
ast_tier_create_of(Ast* ast)
{
	return (AstTierCreate*)ast;
}

static inline AstTierCreate*
ast_tier_create_allocate(void)
{
	AstTierCreate* self;
	self = ast_allocate(0, sizeof(AstTierCreate));
	self->if_not_exists = false;
	self->tier          = NULL;
	str_init(&self->table_name);
	return self;
}

static inline AstTierDrop*
ast_tier_drop_of(Ast* ast)
{
	return (AstTierDrop*)ast;
}

static inline AstTierDrop*
ast_tier_drop_allocate(void)
{
	AstTierDrop* self;
	self = ast_allocate(0, sizeof(AstTierDrop));
	self->if_exists = false;
	str_init(&self->table_name);
	str_init(&self->name);
	return self;
}

static inline AstTierAlter*
ast_tier_alter_of(Ast* ast)
{
	return (AstTierAlter*)ast;
}

static inline AstTierAlter*
ast_tier_alter_allocate(void)
{
	AstTierAlter* self;
	self = ast_allocate(0, sizeof(AstTierAlter));
	self->if_exists             = false;
	self->if_exists_storage     = false;
	self->if_not_exists_storage = false;
	self->type                  = -1;
	self->set                   = NULL;
	self->set_mask              = 0;
	str_init(&self->table_name);
	str_init(&self->name);
	str_init(&self->name_new);
	str_init(&self->name_storage);
	return self;
}

void  parse_tier_create(Stmt*);
void  parse_tier_drop(Stmt*);
void  parse_tier_alter(Stmt*);
Tier* parse_tier(Stmt*, Str*);
