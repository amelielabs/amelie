#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Target     Target;
typedef struct TargetList TargetList;

struct Target
{
	int        id;
	int        level;
	int        level_seq;
	Str        name;
	// aggregates
	AstList    aggs;
	// group by
	AstList    group_by;
	Target*    group_by_target;
	Target*    group_main;
	Target*    group_redirect;
	// expression target
	View*      view;
	Ast*       expr;
	Ast*       expr_on;
	int        rexpr;
	// target
	Ast*       plan;
	Table*     table;
	// link
	Target*    outer;
	Target*    next_join;
	Target*    next;
};

struct TargetList
{
	int     level;
	Target* list;
	Target* list_tail;
	int     count;
};

static inline void
target_init(Target* self, Table* table)
{
	self->id              = 0;
	self->level           = -1;
	self->level_seq       = -1;
	self->group_by_target = NULL;
	self->group_main      = NULL;
	self->group_redirect  = NULL;
	self->view            = NULL;
	self->expr            = NULL;
	self->expr_on         = NULL;
	self->rexpr           = -1;
	self->plan            = NULL;
	self->table           = table;
	self->outer           = NULL;
	self->next_join       = NULL;
	self->next            = NULL;
	str_init(&self->name);
	ast_list_init(&self->aggs);
	ast_list_init(&self->group_by);
}

static inline void
target_group_redirect(Target* self, Target* to)
{
	auto target = self;
	while (target)
	{
		target->group_redirect = to;
		target = target->next_join;
	}
}

static inline bool
target_is_join(Target* self)
{
	return self->next_join != NULL;
}

static inline bool
target_compare(Target* self, Str* name)
{
	return str_compare(&self->name, name);
}

static inline void
target_list_init(TargetList* self)
{
	self->level     = 0;
	self->list      = NULL;
	self->list_tail = NULL;
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
target_list_match_by_id(TargetList* self, int id) 
{
	auto target = self->list;
	while (target)
	{
		if (target->id == id)
			return target;
		target = target->next;
	}
	return NULL;
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

static inline bool
target_list_is_expr(TargetList* self)
{
	auto target = self->list;
	while (target)
	{
		if (target->table)
			return false;
		target = target->next;
	}
	return true;
}

static inline Target*
target_list_first(TargetList* self)
{
	assert(self->count >= 0);
	return target_list_match_by_id(self, 0);
}

static inline Target*
target_list_add(TargetList* self,
                int         level,
                int         level_seq,
                Str*        name,
                Ast*        expr,
                Table*      table)
{
	Target* target = palloc(sizeof(Target));
	target_init(target, table);
	target->id        = self->count;
	target->level     = level;
	target->level_seq = level_seq;
	target->expr      = expr;
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
	return target;
}
