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

static inline Ast*
parse_returning(Stmt* self, Expr* ctx)
{
	// expr [AS] [name], ...
	// *
	Ast* expr_head = NULL;
	Ast* expr_prev = NULL;
	for (auto order = 1;; order++)
	{
		// * or expr
		auto expr = stmt_if(self, '*');
		if (! expr)
			expr = parse_expr(self, ctx);

		// [AS name]
		// [name]
		auto as_has = true;
		auto as = stmt_if(self, KAS);
		if (! as)
		{
			as_has = false;
			as = ast(KAS);
		}

		// set column name
		auto name = stmt_if(self, KNAME);
		if (unlikely(! name))
		{
			if (as_has)
				error("AS <label> expected");

			if (expr->id == KNAME) {
				name = expr;
			} else
			if (expr->id != '*')
			{
				auto name_sz = palloc(8);
				snprintf(name_sz, 8, "col%d", order);
				name = ast(KNAME);
				str_set_cstr(&name->string, name_sz);
			}
		} else
		{
			// ensure * has no alias
			if (expr->id == '*')
				error("<*> cannot have an alias");
		}

		// add column to the select expression list
		as->l = expr;
		as->r = name;
		if (expr_head == NULL)
			expr_head = as;
		else
			expr_prev->next = as;
		expr_prev = as;

		// ,
		if (stmt_if(self, ','))
			continue;

		break;
	}

	return expr_head;
}
