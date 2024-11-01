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

typedef struct Target Target;
typedef struct Stmt   Stmt;

typedef enum
{
	JOIN_NONE,
	JOIN_INNER,
	JOIN_LEFT,
	JOIN_RIGHT
} TargetJoin;

struct Target
{
	int          id;
	int          level;
	int          level_seq;
	int          level_virt;
	Str          name;
	// group by
	AstList      group_by;
	Target*      group_by_target;
	Target*      group_main;
	Target*      group_redirect;
	// expression target
	Cte*         cte;
	View*        view;
	Ast*         expr;
	Ast*         expr_on;
	int          rexpr;
	// target
	Ast*         select;
	Ast*         path;
	Table*       table;
	IndexConfig* index;
	// join
	TargetJoin   join;
	// link
	Target*      outer;
	Target*      next_join;
	Target*      next;
};

static inline void
target_init(Target* self, Table* table)
{
	self->id              = 0;
	self->level           = -1;
	self->level_seq       = -1;
	self->level_virt      = 0;
	self->group_by_target = NULL;
	self->group_main      = NULL;
	self->group_redirect  = NULL;
	self->cte             = NULL;
	self->view            = NULL;
	self->expr            = NULL;
	self->expr_on         = NULL;
	self->rexpr           = -1;
	self->select          = NULL;
	self->path            = NULL;
	self->table           = table;
	self->index           = NULL;
	self->join            = JOIN_NONE;
	self->outer           = NULL;
	self->next_join       = NULL;
	self->next            = NULL;
	str_init(&self->name);
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
