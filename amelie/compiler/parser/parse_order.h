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

typedef struct AstOrder AstOrder;

struct AstOrder
{
	Ast  ast;
	Ast* expr;
	int  order;
	bool asc;
};

static inline AstOrder*
ast_order_of(Ast* ast)
{
	return (AstOrder*)ast;
}

static inline AstOrder*
ast_order_allocate(int order, Ast* expr, bool asc)
{
	AstOrder* self;
	self = ast_allocate(0, sizeof(AstOrder));
	self->expr  = expr;
	self->order = order;
	self->asc   = asc;
	return self;
}

static inline bool
ast_order_list_match_index(AstList* exprs, Target* target)
{
	auto index = target->from_index;
	if (index->keys.list_count != exprs->count)
		return false;

	auto ref  = index->keys.list.next;
	auto node = exprs->list;
	for (; node; node = node->next)
	{
		auto order = ast_order_of(node->ast);
		if (! order->asc)
			return false;

		auto key = container_of(ref, Key, link);
		if (order->expr->id == KNAME)
		{
			// compare key name
			if (! str_compare_case(&key->column->name, &order->expr->string))
				return false;
		} else
		if (order->expr->id == KNAME_COMPOUND)
		{
			// target.key
			Str name;
			str_split(&order->expr->string, &name, '.');
			Str name_key = order->expr->string;
			str_advance(&name_key, str_size(&name) + 1);

			// compare target name
			if (! str_compare_case(&name, &target->name))
				return false;

			// compare key name
			if (! str_compare_case(&key->column->name, &name_key))
				return false;
		} else {
			return false;
		}

		ref = ref->next;
	}
	return true;
}
