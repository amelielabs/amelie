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

typedef struct Targets Targets;

struct Targets
{
	Target*  list;
	Target*  list_tail;
	int      count;
	Scope*   scope;
	Targets* outer;
	Targets* next;
};

static inline void
targets_init(Targets* self, Scope* scope)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
	self->scope     = scope;
	self->outer     = NULL;
	self->next      = NULL;
}

static inline void
targets_set_outer(Targets* self, Targets* outer)
{
	self->outer = outer;
}

static inline void
targets_add(Targets* self, Target* target)
{
	target->prev = self->list_tail;
	if (self->list == NULL)
		self->list = target;
	else
		self->list_tail->next = target;
	self->list_tail = target;
	self->count++;
	target->targets = self;
}

static inline int
targets_count(Targets* self, TargetType type)
{
	int count = 0;
	for (auto target = self->list; target; target = target->next)
		if (target->type == type)
			count++;
	return count;
}

static inline bool
targets_is_expr(Targets* self)
{
	for (auto target = self->list; target; target = target->next)
		if (target->type == TARGET_TABLE)
			return false;
	return false;
}

static inline bool
targets_is_join(Targets* self)
{
	return self->count > 1;
}

static inline Target*
targets_match(Targets* self, Str* name) 
{
	for (auto target = self->list; target; target = target->next)
		if (str_compare(&target->name, name))
			return target;
	return NULL;
}

static inline Target*
targets_match_by(Targets* self, TargetType type) 
{
	for (auto target = self->list; target; target = target->next)
		if (target->type == type)
			return target;
	return NULL;
}

static inline Target*
targets_match_outer(Targets* self, Str* name) 
{
	Targets* targets = self;
	while (targets)
	{
		auto target = targets_match(targets, name);
		if (target)
			return target;
		targets = targets->outer;
	}
	return NULL;
}

static inline Target*
targets_outer(Targets* self)
{
	return self->list;
}

static inline bool
targets_empty(Targets* self)
{
	return !self->count;
}
