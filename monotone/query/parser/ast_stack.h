#pragma once

//
// monotone
//
// SQL OLTP database
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

static inline Ast*
ast_tail(AstStack* self)
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

static inline Ast*
ast_slice(AstStack* self, Ast* after)
{
	// not including after
	Ast* start = NULL;
	if (after == NULL)
	{
		start = self->list;
		ast_stack_init(self);
	} else
	{
		start = after->next;
		start->prev = NULL;
		after->next = NULL;
		self->list_tail = after;
	}
	return start;
}
