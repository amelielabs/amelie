#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct AstGroup AstGroup;

struct AstGroup
{
	Ast  ast;
	int  order;
	Ast* expr;
	Ast* label;
};

static inline AstGroup*
ast_group_of(Ast* ast)
{
	return (AstGroup*)ast;
}

static inline AstGroup*
ast_group_allocate(int order, Ast* expr, Ast* label)
{
	AstGroup* self;
	self = ast_allocate(0, sizeof(AstGroup));
	self->order = order;
	self->expr  = expr;
	self->label = label;
	return self;
}

static inline AstGroup*
ast_group_match(AstList* list, Str* path)
{
	auto node = list->list;
	while (node)
	{
		auto group = ast_group_of(node->ast);
		if (group->label)
			if (str_compare(&group->label->string, path))
				return group;
		node = node->next;
	}
	return NULL;
}
