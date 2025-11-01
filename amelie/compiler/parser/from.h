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

typedef struct From From;

struct From
{
	Target* list;
	Target* list_tail;
	int     count;
	Ast*    join_on;
	AstList list_names;
	Block*  block;
	From*   outer;
};

static inline void
from_init(From* self, Block* block)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
	self->join_on   = NULL;
	self->block     = block;
	self->outer     = NULL;
	ast_list_init(&self->list_names);
}

static inline void
from_set_outer(From* self, From* outer)
{
	self->outer = outer;
}

static inline void
from_add(From* self, Target* target)
{
	target->prev = self->list_tail;
	if (self->list == NULL)
		self->list = target;
	else
		self->list_tail->next = target;
	self->list_tail = target;
	self->count++;
	target->from = self;
}

static inline Target*
from_first(From* self)
{
	return self->list;
}

static inline bool
from_empty(From* self)
{
	return !self->count;
}

static inline bool
from_is_expr(From* self)
{
	for (auto target = self->list; target; target = target->next)
		if (target->type == TARGET_TABLE)
			return false;
	return false;
}

static inline bool
from_is_join(From* self)
{
	return self->count > 1;
}

static inline Target*
from_match(From* self, Str* name)
{
	for (auto target = self->list; target; target = target->next)
		if (str_compare(&target->name, name))
			return target;
	return NULL;
}

static inline Target*
from_match_by(From* self, TargetType type)
{
	for (auto target = self->list; target; target = target->next)
		if (target->type == type)
			return target;
	return NULL;
}
