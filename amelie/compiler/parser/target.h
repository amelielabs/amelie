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

typedef struct Target  Target;
typedef struct Targets Targets;
typedef struct Stmt    Stmt;
typedef struct Plan    Plan;

typedef enum
{
	TARGET_NONE,
	TARGET_TABLE_SHARED,
	TARGET_TABLE,
	TARGET_SELECT,
	TARGET_FUNCTION,
	TARGET_GROUP_BY,
	TARGET_CTE,
	TARGET_VALUES
} TargetType;

typedef enum
{
	JOIN_NONE,
	JOIN_INNER,
	JOIN_LEFT,
	JOIN_RIGHT
} TargetJoin;

struct Target
{
	TargetType   type;
	int          id;
	Str          name;
	Ast*         ast;
	// target
	Table*       from_table;
	IndexConfig* from_table_index;
	Ast*         from_select;
	AstList*     from_group_by;
	Stmt*        from_cte;
	Columns*     from_columns;
	Ast*         from_function;
	int          r;
	// target plans
	Plan*        plan;
	Plan*        plan_primary;
	// join
	TargetJoin   join;
	Ast*         join_on;
	// inner/outer
	Target*      next;
	Target*      prev;
	Targets*     targets;
};

static inline Target*
target_allocate(int* id_seq)
{
	Target* self = palloc(sizeof(Target));
	self->type             = TARGET_NONE;
	self->id               = *id_seq;
	self->from_table       = NULL;
	self->from_table_index = NULL;
	self->from_select      = NULL;
	self->from_group_by    = NULL;
	self->from_cte         = NULL;
	self->from_columns     = NULL;
	self->from_function    = NULL;
	self->r                = -1;
	self->plan             = NULL;
	self->plan_primary     = NULL;
	self->join             = JOIN_NONE;
	self->join_on          = NULL;
	self->next             = NULL;
	self->prev             = NULL;
	self->targets          = NULL;
	str_init(&self->name);
	(*id_seq)++;
	return self;
}

static inline bool
target_is_table(Target* self)
{
	return self->type == TARGET_TABLE ||
	       self->type == TARGET_TABLE_SHARED;
}
