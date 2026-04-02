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

typedef struct AstStreamCreate AstStreamCreate;
typedef struct AstStreamDrop   AstStreamDrop;
typedef struct AstStreamAlter  AstStreamAlter;

struct AstStreamCreate
{
	Ast           ast;
	bool          if_not_exists;
	StreamConfig* config;
};

struct AstStreamDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstStreamAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

static inline AstStreamCreate*
ast_stream_create_of(Ast* ast)
{
	return (AstStreamCreate*)ast;
}

static inline AstStreamCreate*
ast_stream_create_allocate(void)
{
	AstStreamCreate* self;
	self = ast_allocate(0, sizeof(AstStreamCreate));
	return self;
}

static inline AstStreamDrop*
ast_stream_drop_of(Ast* ast)
{
	return (AstStreamDrop*)ast;
}

static inline AstStreamDrop*
ast_stream_drop_allocate(void)
{
	AstStreamDrop* self;
	self = ast_allocate(0, sizeof(AstStreamDrop));
	return self;
}

static inline AstStreamAlter*
ast_stream_alter_of(Ast* ast)
{
	return (AstStreamAlter*)ast;
}

static inline AstStreamAlter*
ast_stream_alter_allocate(void)
{
	AstStreamAlter* self;
	self = ast_allocate(0, sizeof(AstStreamAlter));
	return self;
}

void parse_stream_create(Stmt*);
void parse_stream_drop(Stmt*);
void parse_stream_alter(Stmt*);
