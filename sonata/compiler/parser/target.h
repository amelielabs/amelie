#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Target Target;
typedef struct Stmt   Stmt;

typedef enum
{
	TARGET_JOIN_INNER,
	TARGET_JOIN_LEFT,
	TARGET_JOIN_RIGHT
} TargetJoin;

struct Target
{
	int          id;
	int          level;
	int          level_seq;
	Str          name;
	// group by
	AstList      group_by;
	Target*      group_by_target;
	Target*      group_main;
	Target*      group_redirect;
	// expression target
	Stmt*        cte;
	View*        view;
	Ast*         expr;
	Ast*         expr_on;
	int          rexpr;
	// target
	Ast*         select;
	Ast*         path;
	Table*       table;
	IndexConfig* index;
	// link
	TargetJoin   join;
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
	self->join            = TARGET_JOIN_INNER;
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
