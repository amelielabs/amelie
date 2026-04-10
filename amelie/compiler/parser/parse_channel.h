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

typedef struct AstChannelCreate AstChannelCreate;
typedef struct AstChannelDrop   AstChannelDrop;
typedef struct AstChannelAlter  AstChannelAlter;

struct AstChannelCreate
{
	Ast            ast;
	bool           if_not_exists;
	ChannelConfig* config;
};

struct AstChannelDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstChannelAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

static inline AstChannelCreate*
ast_channel_create_of(Ast* ast)
{
	return (AstChannelCreate*)ast;
}

static inline AstChannelCreate*
ast_channel_create_allocate(void)
{
	AstChannelCreate* self;
	self = ast_allocate(0, sizeof(AstChannelCreate));
	return self;
}

static inline AstChannelDrop*
ast_channel_drop_of(Ast* ast)
{
	return (AstChannelDrop*)ast;
}

static inline AstChannelDrop*
ast_channel_drop_allocate(void)
{
	AstChannelDrop* self;
	self = ast_allocate(0, sizeof(AstChannelDrop));
	return self;
}

static inline AstChannelAlter*
ast_channel_alter_of(Ast* ast)
{
	return (AstChannelAlter*)ast;
}

static inline AstChannelAlter*
ast_channel_alter_allocate(void)
{
	AstChannelAlter* self;
	self = ast_allocate(0, sizeof(AstChannelAlter));
	return self;
}

void parse_channel_create(Stmt*);
void parse_channel_drop(Stmt*);
void parse_channel_alter(Stmt*);
