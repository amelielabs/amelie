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

void parse_from(Stmt*, Targets*, AccessType, bool);

static inline Ast*
parse_from_join_on_and_where(Targets* targets, Ast* expr_where)
{
	for (auto target = targets->list; target; target = target->next)
	{
		if (! target->join_on)
			continue;
		if (expr_where == NULL) {
			expr_where = target->join_on;
		} else
		{
			auto and = ast(KAND);
			and->l = expr_where;
			and->r = target->join_on;
			expr_where = and;
		}
	}
	return expr_where;
}
