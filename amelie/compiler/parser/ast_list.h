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

typedef struct AstListNode AstListNode;
typedef struct AstList     AstList;

struct AstListNode
{
	Ast*         ast;
	AstListNode* next;
	AstListNode* prev;
};

struct AstList
{
	AstListNode* list;
	AstListNode* list_tail;
	int          count;
};

static inline void
ast_list_init(AstList* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline Ast*
ast_list_last(AstList* self)
{
	assert(self->list_tail);
	return self->list_tail->ast;
}

static inline void
ast_list_add(AstList* self, Ast* ast)
{
	AstListNode* node;
	node = palloc(sizeof(AstListNode));
	node->ast  = ast;
	node->next = NULL;
	node->prev = self->list_tail;
	if (self->list == NULL)
		self->list = node;
	else
		self->list_tail->next = node;
	self->list_tail = node;
	self->count++;
}
