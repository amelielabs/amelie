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
	TARGET_NONE         = 0,
	TARGET_TABLE_SHARED = 1 << 0,
	TARGET_TABLE        = 1 << 1,
	TARGET_SELECT       = 1 << 2,
	TARGET_GROUP_BY     = 1 << 3,
	TARGET_CTE          = 1 << 4,
	TARGET_INSERTED     = 1 << 5,
	TARGET_VIEW         = 1 << 6
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
	int          level;
	int          level_seq;
	Str          name;
	// target
	Table*       from_table;
	IndexConfig* from_table_index;
	Ast*         from_select;
	Cte*         from_cte;
	View*        from_view;
	Columns*     from_columns;
	int          r;
	// target keys
	Ast*         path;
	// join
	TargetJoin   join;
	Ast*         join_on;
	// link
	Target*      outer;
	Target*      next_join;
	Target*      next;
};

static inline Target*
target_allocate(void)
{
	Target* self = palloc(sizeof(Target));
	self->type             = TARGET_NONE;
	self->id               = 0;
	self->level            = -1;
	self->level_seq        = -1;
	self->from_table       = NULL;
	self->from_table_index = NULL;
	self->from_select      = NULL;
	self->from_cte         = NULL;
	self->from_view        = NULL;
	self->from_columns     = NULL;
	self->r                = -1;
	self->path             = NULL;
	self->join             = JOIN_NONE;
	self->join_on          = NULL;
	self->outer            = NULL;
	self->next_join        = NULL;
	self->next             = NULL;
	str_init(&self->name);
	return self;
}

static inline bool
target_is_join(Target* self)
{
	return self->next_join != NULL;
}
