#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct AstShow        AstShow;
typedef struct AstSet         AstSet;
typedef struct AstCreateUser  AstCreateUser;
typedef struct AstCreateTable AstCreateTable;
typedef struct AstDropUser    AstDropUser;
typedef struct AstDropTable   AstDropTable;
typedef struct AstInsert      AstInsert;
typedef struct AstSelect      AstSelect;

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

struct AstCreateUser
{
	Ast         ast;
	bool        if_not_exists;
	UserConfig* config;
};

struct AstCreateTable
{
	Ast          ast;
	bool         if_not_exists;
	TableConfig* config;
};

struct AstDropUser
{
	Ast  ast;
	bool if_exists;
	Ast* name;
};

struct AstDropTable
{
	Ast  ast;
	bool if_exists;
	Ast* name;
};

struct AstInsert
{
	Ast    ast;
	bool   unique;
	Table* table;
	Ast*   rows;
	int    rows_count;
	bool   generate;
	Ast*   generate_count;
	Ast*   generate_batch;
};

struct AstSelect
{
	Ast  ast;
	Ast* expr;
};

// show
static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline AstShow*
ast_show_allocate(Ast* expr)
{
	AstShow* self = ast_allocate(KSHOW, sizeof(AstShow));
	self->expr = expr;
	return self;
}

// set
static inline AstSet*
ast_set_of(Ast* ast)
{
	return (AstSet*)ast;
}

static inline AstSet*
ast_set_allocate(Ast* name, Ast* value)
{
	AstSet* self = ast_allocate(KSET, sizeof(AstSet));
	self->name  = name;
	self->value = value;
	return self;
}

// create user
static inline AstCreateUser*
ast_create_user_of(Ast* ast)
{
	return (AstCreateUser*)ast;
}

static inline AstCreateUser*
ast_create_user_allocate(void)
{
	AstCreateUser* self;
	self = ast_allocate(KCREATE_USER, sizeof(AstCreateUser));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

// create table
static inline AstCreateTable*
ast_create_table_of(Ast* ast)
{
	return (AstCreateTable*)ast;
}

static inline AstCreateTable*
ast_create_table_allocate(void)
{
	AstCreateTable* self;
	self = ast_allocate(KCREATE_TABLE, sizeof(AstCreateTable));
	self->if_not_exists = false;
	self->config        = NULL;
	return self;
}

// drop user
static inline AstDropUser*
ast_drop_user_of(Ast* ast)
{
	return (AstDropUser*)ast;
}

static inline AstDropUser*
ast_drop_user_allocate(void)
{
	AstDropUser* self;
	self = ast_allocate(KDROP_USER, sizeof(AstDropUser));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

// drop table
static inline AstDropTable*
ast_drop_table_of(Ast* ast)
{
	return (AstDropTable*)ast;
}

static inline AstDropTable*
ast_drop_table_allocate(void)
{
	AstDropTable* self;
	self = ast_allocate(KDROP_TABLE, sizeof(AstDropTable));
	self->if_exists = false;
	self->name      = NULL;
	return self;
}

// insert
static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline AstInsert*
ast_insert_allocate(void)
{
	AstInsert* self;
	self = ast_allocate(KINSERT, sizeof(AstInsert));
	self->unique         = false;
	self->table          = NULL;
	self->rows           = NULL;
	self->rows_count     = 0;
	self->generate       = false;
	self->generate_count = NULL;
	self->generate_batch = NULL;
	return self;
}

#if 0
// select
static inline AstSelect*
ast_select_of(Ast* ast)
{
	return (AstSelect*)ast;
}

static inline Ast*
ast_select_allocate(void)
{
	AstSelect* self;
	self = ast_allocate(KSELECT, sizeof(AstSelect));
	self->expr = NULL;
	return &self->ast;
}
#endif
