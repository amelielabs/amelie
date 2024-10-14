#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct CteDepsNode CteDepsNode;
typedef struct CteDeps     CteDeps;

struct CteDepsNode
{
	Cte*         cte;
	CteDepsNode* next;
};

struct CteDeps
{
	CteDepsNode* list;
	CteDepsNode* list_tail;
	int          count;
};

static inline void
cte_deps_init(CteDeps* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
cte_deps_add(CteDeps* self, Cte* cte)
{
	CteDepsNode* node;
	node = palloc(sizeof(CteDepsNode));
	node->cte  = cte;
	node->next = NULL;
	if (self->list == NULL)
		self->list = node;
	else
		self->list_tail->next = node;
	self->list_tail = node;
	self->count++;
}

static inline int
cte_deps_max_stmt(CteDeps* self)
{
	auto order = -1;
	auto node  = self->list;
	while (node)
	{
		if (node->cte->stmt > order)
			order = node->cte->stmt;
		node = node->next;
	}
	return order;
}
