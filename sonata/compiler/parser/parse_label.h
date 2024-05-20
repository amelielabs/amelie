#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct AstLabel AstLabel;

struct AstLabel
{
	Ast  ast;
	int  order;
	Ast* name;
	Ast* expr;
};

static inline AstLabel*
ast_label_of(Ast* ast)
{
	return (AstLabel*)ast;
}

static inline AstLabel*
ast_label_allocate(Ast* name, Ast* expr, int order)
{
	AstLabel* self;
	self = ast_allocate(0, sizeof(AstLabel));
	self->order = order;
	self->name  = name;
	self->expr  = expr;
	return self;
}

static inline AstLabel*
ast_label_match(AstList* list, Str* name)
{
	auto node = list->list;
	while (node)
	{
		auto label = ast_label_of(node->ast);
		if (str_compare(&label->name->string, name))
			return label;
		node = node->next;
	}
	return NULL;
}
