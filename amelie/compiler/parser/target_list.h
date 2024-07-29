#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct TargetList TargetList;

enum
{
	TARGET_NONE         = 0,
	TARGET_EXPR         = 1 << 0,
	TARGET_TABLE_SHARED = 1 << 1,
	TARGET_TABLE        = 1 << 2,
	TARGET_CTE          = 1 << 3
};

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

static inline Target*
target_list_add(TargetList* self,
                int         level,
                int         level_seq,
                Str*        name,
                Ast*        expr,
                Table*      table,
                Stmt*       cte)
{
	Target* target = palloc(sizeof(Target));
	target_init(target, table);
	target->id        = self->count;
	target->level     = level;
	target->level_seq = level_seq;
	target->expr      = expr;
	target->cte       = cte;
	if (! name)
	{
		str_init(&target->name);
	} else
	{
		int   copy_size = str_size(name);
		char* copy = palloc(copy_size + 1);
		memcpy(copy, str_of(name), copy_size);
		copy[copy_size] = 0;
		str_init(&target->name);
		str_set(&target->name, copy, copy_size);
	}
	if (self->list == NULL)
		self->list = target;
	else
		self->list_tail->next = target;
	self->list_tail = target;
	self->count++;

	if (expr) {
		self->state |= TARGET_EXPR;
	} else
	if (table)
	{
		if (table->config->shared)
			self->state |= TARGET_TABLE_SHARED;
		else
			self->state |= TARGET_TABLE;
	} else
	if (cte) {
		self->state |= TARGET_CTE;
	}
	return target;
}

static inline bool
target_list_has(TargetList* self, int state)
{
	return (self->state & state) > 0;
}

static inline bool
target_list_expr(TargetList* self)
{
	return self->state == TARGET_NONE ||
	       self->state == TARGET_EXPR ||
	       self->state == TARGET_CTE;
}

static inline Target*
target_list_match(TargetList* self, Str* name) 
{
	auto target = self->list;
	while (target)
	{
		if (target_compare(target, name))
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

		// including views and cte
		if (target->expr || target->cte)
			continue;

		// skip group by targets
		if (target->group_main)
			continue;

		if (target->table->config->shared)
			continue;

		error("subqueries to distributed tables are not supported");
	}
}

static inline void
target_list_validate(TargetList* self, Target* primary)
{
	auto table = primary->table;
	assert(table);

	// SELECT FROM table

	// validate supported targets as expression or shared table
	target_list_validate_subqueries(self, primary);
}

static inline void
target_list_validate_dml(TargetList* self, Target* primary)
{
	auto table = primary->table;
	assert(table);

	// DELETE table, ...
	// UPDATE FROM table, ...

	// ensure we are not joining with self
	auto target = primary->next_join;
	while (target)
	{
		if (target->table == table)
			error("DML JOIN using the same table are not supported");
		target = target->next_join;
	}

	// validate supported targets as expression or shared table
	target_list_validate_subqueries(self, primary);
}
