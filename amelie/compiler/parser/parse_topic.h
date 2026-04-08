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

typedef struct AstTopicCreate AstTopicCreate;
typedef struct AstTopicDrop   AstTopicDrop;
typedef struct AstTopicAlter  AstTopicAlter;

struct AstTopicCreate
{
	Ast          ast;
	bool         if_not_exists;
	TopicConfig* config;
};

struct AstTopicDrop
{
	Ast  ast;
	bool if_exists;
	Str  name;
};

struct AstTopicAlter
{
	Ast  ast;
	bool if_exists;
	Str  name;
	Str  name_new;
};

static inline AstTopicCreate*
ast_topic_create_of(Ast* ast)
{
	return (AstTopicCreate*)ast;
}

static inline AstTopicCreate*
ast_topic_create_allocate(void)
{
	AstTopicCreate* self;
	self = ast_allocate(0, sizeof(AstTopicCreate));
	return self;
}

static inline AstTopicDrop*
ast_topic_drop_of(Ast* ast)
{
	return (AstTopicDrop*)ast;
}

static inline AstTopicDrop*
ast_topic_drop_allocate(void)
{
	AstTopicDrop* self;
	self = ast_allocate(0, sizeof(AstTopicDrop));
	return self;
}

static inline AstTopicAlter*
ast_topic_alter_of(Ast* ast)
{
	return (AstTopicAlter*)ast;
}

static inline AstTopicAlter*
ast_topic_alter_allocate(void)
{
	AstTopicAlter* self;
	self = ast_allocate(0, sizeof(AstTopicAlter));
	return self;
}

void parse_topic_create(Stmt*);
void parse_topic_drop(Stmt*);
void parse_topic_alter(Stmt*);
