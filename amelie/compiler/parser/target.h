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
typedef struct Path    Path;

typedef enum
{
	TARGET_NONE,
	TARGET_TABLE,
	TARGET_FUNCTION,
	TARGET_EXPR,
	TARGET_GROUP_BY,
	TARGET_CTE,
	TARGET_VAR,
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
	Str          name;
	Ast*         ast;
	// target src
	union {
		struct {
			AccessType   from_access;
			Table*       from_table;
			IndexConfig* from_index;
			bool         from_heap;
		};
		AstList* from_group_by;
		Stmt*    from_cte;
		Var*     from_var;
		Ast*     from_function;
	};
	// target
	Columns*     columns;
	int          r;
	int          rcursor;
	// target path
	Path*        path;
	Path*        path_primary;
	// join
	TargetJoin   join;
	Ast*         join_on;
	// inner/outer
	Target*      next;
	Target*      prev;
	Targets*     targets;
};

static inline Target*
target_allocate(void)
{
	Target* self = palloc(sizeof(Target));
	memset(self, 0, sizeof(Target));
	self->type        = TARGET_NONE;
	self->from_access = ACCESS_UNDEF;
	self->columns     = NULL;
	self->r           = -1;
	self->rcursor     = -1;
	self->join        = JOIN_NONE;
	str_init(&self->name);
	return self;
}

static inline bool
target_is_table(Target* self)
{
	return self->type == TARGET_TABLE;
}
