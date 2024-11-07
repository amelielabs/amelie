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

typedef struct AstStack AstStack;

struct AstStack
{
	Ast* list;
	Ast* list_tail;
};

static inline void
ast_stack_init(AstStack* self)
{
	self->list = NULL;
	self->list_tail = NULL;
}

static inline void
ast_stack_reset(AstStack* self)
{
	self->list = NULL;
	self->list_tail = NULL;
}

static inline Ast*
ast_head(AstStack* self)
{
	return self->list_tail;
}

static inline void
ast_push(AstStack* self, Ast* ast)
{
	if (self->list == NULL)
		self->list = ast;
	else
		self->list_tail->next = ast;
	ast->prev = self->list_tail;
	self->list_tail = ast;
}

static inline Ast*
ast_pop(AstStack* self)
{
	if (unlikely(self->list_tail == NULL))
		return NULL;
	auto head = self->list_tail;
	self->list_tail = head->prev;
	if (self->list_tail == NULL)
		self->list = NULL;
	else
		self->list_tail->next = NULL;
	return head;
}
