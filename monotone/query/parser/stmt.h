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
	Ast  ast;
	Ast* table;
	Ast* values;
	int  values_count;
};

// show
static inline AstShow*
ast_show_of(Ast* ast)
{
	return (AstShow*)ast;
}

static inline Ast*
ast_show_allocate(Ast* expr)
{
	AstShow* self = ast_allocate(KSHOW, sizeof(AstShow));
	self->expr = expr;
	return &self->ast;
}

// set
static inline AstSet*
ast_set_of(Ast* ast)
{
	return (AstSet*)ast;
}

static inline Ast*
ast_set_allocate(Ast* name, Ast* value)
{
	AstSet* self = ast_allocate(KSET, sizeof(AstSet));
	self->name  = name;
	self->value = value;
	return &self->ast;
}

// create user
static inline AstCreateUser*
ast_create_user_of(Ast* ast)
{
	return (AstCreateUser*)ast;
}

static inline Ast*
ast_create_user_allocate(void)
{
	AstCreateUser* self;
	self = ast_allocate(KCREATE_USER, sizeof(AstCreateUser));
	self->if_not_exists = false;
	self->config        = NULL;
	return &self->ast;
}

// create table
static inline AstCreateTable*
ast_create_table_of(Ast* ast)
{
	return (AstCreateTable*)ast;
}

static inline Ast*
ast_create_table_allocate(void)
{
	AstCreateTable* self;
	self = ast_allocate(KCREATE_TABLE, sizeof(AstCreateTable));
	self->if_not_exists = false;
	self->config        = NULL;
	return &self->ast;
}

// drop user
static inline AstDropUser*
ast_drop_user_of(Ast* ast)
{
	return (AstDropUser*)ast;
}

static inline Ast*
ast_drop_user_allocate(void)
{
	AstDropUser* self;
	self = ast_allocate(KDROP_USER, sizeof(AstDropUser));
	self->if_exists = false;
	self->name      = NULL;
	return &self->ast;
}

// drop table
static inline AstDropTable*
ast_drop_table_of(Ast* ast)
{
	return (AstDropTable*)ast;
}

static inline Ast*
ast_drop_table_allocate(void)
{
	AstDropTable* self;
	self = ast_allocate(KDROP_TABLE, sizeof(AstDropTable));
	self->if_exists = false;
	self->name      = NULL;
	return &self->ast;
}

// insert
static inline AstInsert*
ast_insert_of(Ast* ast)
{
	return (AstInsert*)ast;
}

static inline Ast*
ast_insert_allocate(void)
{
	AstInsert* self;
	self = ast_allocate(KINSERT, sizeof(AstInsert));
	self->table        = NULL;
	self->values       = NULL;
	self->values_count = 0;
	return &self->ast;
}
