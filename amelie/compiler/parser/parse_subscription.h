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

typedef struct AstSubCreate AstSubCreate;
typedef struct AstSubDrop   AstSubDrop;
typedef struct AstSubAlter  AstSubAlter;
typedef struct AstAck       AstAck;

struct AstSubCreate
{
	Ast        ast;
	bool       if_not_exists;
	SubConfig* config;
};

struct AstSubDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstSubAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

struct AstAck
{
	Ast      ast;
	Str      name;
	uint64_t to_lsn;
	uint64_t to_op;
	Sub*     sub;
};

static inline AstSubCreate*
ast_sub_create_of(Ast* ast)
{
	return (AstSubCreate*)ast;
}

static inline AstSubCreate*
ast_sub_create_allocate(void)
{
	AstSubCreate* self;
	self = ast_allocate(0, sizeof(AstSubCreate));
	return self;
}

static inline AstSubDrop*
ast_sub_drop_of(Ast* ast)
{
	return (AstSubDrop*)ast;
}

static inline AstSubDrop*
ast_sub_drop_allocate(void)
{
	AstSubDrop* self;
	self = ast_allocate(0, sizeof(AstSubDrop));
	return self;
}

static inline AstSubAlter*
ast_sub_alter_of(Ast* ast)
{
	return (AstSubAlter*)ast;
}

static inline AstSubAlter*
ast_sub_alter_allocate(void)
{
	AstSubAlter* self;
	self = ast_allocate(0, sizeof(AstSubAlter));
	return self;
}

static inline AstAck*
ast_ack_of(Ast* ast)
{
	return (AstAck*)ast;
}

static inline AstAck*
ast_ack_allocate(void)
{
	AstAck* self;
	self = ast_allocate(0, sizeof(AstAck));
	return self;
}

void parse_sub_create(Stmt*);
void parse_sub_drop(Stmt*);
void parse_sub_alter(Stmt*);
void parse_acknowledge(Stmt*);
