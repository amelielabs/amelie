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

typedef struct AstGroup AstGroup;

struct AstGroup
{
	Ast     ast;
	int     order;
	Ast*    expr;
	Column* column;
};

static inline AstGroup*
ast_group_of(Ast* ast)
{
	return (AstGroup*)ast;
}

static inline AstGroup*
ast_group_allocate(int order, Ast* expr)
{
	AstGroup* self;
	self = ast_allocate(KAGGRKEY, sizeof(AstGroup));
	self->order  = order;
	self->expr   = expr;
	self->column = NULL;
	return self;
}

static inline AstGroup*
ast_group_resolve_column(AstList* list, Str* name)
{
	for (auto node = list->list; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);
		if (group->expr->id != KNAME &&
		    group->expr->id != KNAME_COMPOUND)
			continue;
		if (str_compare(&group->expr->string, name))
			return group;
	}
	return NULL;
}

static inline AstGroup*
ast_group_resolve_column_prefix(AstList* list, Str* name)
{
	// search name.path in group by name (group by expr has prefix of the path)
	for (auto node = list->list; node; node = node->next)
	{
		auto group = ast_group_of(node->ast);
		if (group->expr->id != KNAME_COMPOUND)
			continue;
		if (str_compare_prefix(name, &group->expr->string))
			return group;
	}
	return NULL;
}
