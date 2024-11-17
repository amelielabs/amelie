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

typedef struct TargetList TargetList;

struct TargetList
{
	int     state;
	Target* list;
	Target* list_tail;
	int     level;
	int     count;
};

static inline void
target_list_init(TargetList* self)
{
	self->state     = TARGET_NONE;
	self->list      = NULL;
	self->list_tail = NULL;
	self->level     = 0;
	self->count     = 0;
}

static inline void
target_list_reset(TargetList* self)
{
	target_list_init(self);
}

static inline int
target_list_next_level(TargetList* self)
{
	return self->level++;
}

static inline void
target_list_add(TargetList* self,
                Target*     target,
                int         level,
                int         level_seq)
{
	target->id        = self->count;
	target->level     = level;
	target->level_seq = level_seq;
	if (self->list == NULL)
		self->list = target;
	else
		self->list_tail->next = target;
	self->list_tail = target;
	self->count++;
	self->state |= target->type;
}

static inline bool
target_list_has(TargetList* self, int state)
{
	return (self->state & state) > 0;
}

static inline bool
target_list_is_expr(TargetList* self)
{
	return !target_list_has(self, TARGET_TABLE) &&
	       !target_list_has(self, TARGET_TABLE_SHARED);
}

static inline Target*
target_list_match(TargetList* self, Str* name) 
{
	auto target = self->list;
	while (target)
	{
		if (str_compare(&target->name, name))
			return target;
		target = target->next;
	}
	return NULL;
}

static inline void
target_list_validate_subqueries(TargetList* self, Target* primary)
{
	auto target = self->list;
	for (; target; target = target->next)
	{
		if (target == primary)
			continue;

		if (target->type == TARGET_TABLE)
			error("subqueries to distributed tables are not supported");
	}
}

static inline void
target_list_validate(TargetList* self, Target* primary)
{
	auto table = primary->from_table;
	unused(table);
	assert(table);

	// SELECT FROM table

	// validate supported targets as expression or shared table
	target_list_validate_subqueries(self, primary);
}

static inline void
target_list_validate_dml(TargetList* self, Target* primary)
{
	auto table = primary->from_table;
	assert(table);

	// DELETE table, ...
	// UPDATE FROM table, ...

	// ensure we are not joining with self
	auto target = primary->next_join;
	while (target)
	{
		if (target->from_table == table)
			error("DML JOIN using the same table are not supported");
		target = target->next_join;
	}

	// validate supported targets as expression or shared table
	target_list_validate_subqueries(self, primary);
}
