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

Target* parse_from(Stmt*, int);

static inline Ast*
parse_from_join_on_and_where(Target* target, Ast* expr_where)
{
	while (target)
	{
		if (target->join_on)
		{
			if (expr_where == NULL)
			{
				expr_where = target->join_on;
			} else
			{
				auto and = ast(KAND);
				and->l = expr_where;
				and->r = target->join_on;
				expr_where = and;
			}
		}
		target = target->next_join;
	}
	return expr_where;
}
