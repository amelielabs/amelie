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

typedef struct StmtList StmtList;

struct StmtList
{
	Stmt* list;
	Stmt* list_tail;
	int   count;
};

static inline void
stmt_list_init(StmtList* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline void
stmt_list_add(StmtList* self, Stmt* stmt)
{
	if (self->list == NULL)
		self->list = stmt;
	else
		self->list_tail->next = stmt;
	stmt->prev = self->list_tail;
	self->list_tail = stmt;
	self->count++;
}

static inline void
stmt_list_insert(StmtList* self, Stmt* before, Stmt* stmt)
{
	if (before->prev == NULL)
		self->list = stmt;
	else
		before->prev->next = stmt;
	stmt->prev = before->prev;
	stmt->next = before;
	before->prev = stmt;
	self->count++;
}

static inline void
stmt_list_order(StmtList* self)
{
	int order = 0;
	for (auto stmt = self->list; stmt; stmt = stmt->next)
		stmt->order = order++;
}

static inline Stmt*
stmt_list_find(StmtList* self, Str* name)
{
	for (auto stmt = self->list; stmt; stmt = stmt->next)
	{
		if (stmt->cte_name == NULL)
			continue;
		if (str_compare(&stmt->cte_name->string, name))
			return stmt;
	}
	return NULL;
}
