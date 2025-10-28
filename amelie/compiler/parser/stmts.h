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

typedef struct Stmts Stmts;

struct Stmts
{
	Stmt* list;
	Stmt* list_tail;
	Stmt* last_send;
	int   count;
	int   count_utility;
	int   count_break;
	int   count_continue;
};

static inline void
stmts_init(Stmts* self)
{
	self->list           = NULL;
	self->list_tail      = NULL;
	self->last_send      = NULL;
	self->count          = 0;
	self->count_utility  = 0;
	self->count_break    = 0;
	self->count_continue = 0;
}

static inline void
stmts_add(Stmts* self, Stmt* stmt)
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
stmts_insert(Stmts* self, Stmt* before, Stmt* stmt)
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

static inline Stmt*
stmts_find(Stmts* self, Str* name)
{
	for (auto stmt = self->list; stmt; stmt = stmt->next)
	{
		if (! stmt->cte_name)
			continue;
		if (str_compare(stmt->cte_name, name))
			return stmt;
	}
	return NULL;
}
