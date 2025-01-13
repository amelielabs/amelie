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

hot static inline void
ops_extract(AstList* self, Ast* expr)
{
	switch (expr->id) {
	case KAND:
	{
		ops_extract(self, expr->l);
		ops_extract(self, expr->r);
		break;
	}
	case KOR:
	{
		// skipped
		break;
	}
	case KGTE:
	case KLTE:
	case '>':
	case '<':
	case '=':
	{
		// [target.]name = value | [target.]name
		Ast* value;
		if (expr->l->id == KNAME || expr->l->id == KNAME_COMPOUND)
			value = expr->r;
		else
		if (expr->r->id == KNAME || expr->r->id == KNAME_COMPOUND)
			value = expr->l;
		else
			break;
		if (value->id == KINT       ||
		    value->id == KSTRING    ||
		    value->id == KTIMESTAMP ||
		    value->id == KUUID      ||
		    value->id == KNAME      ||
		    value->id == KNAME_COMPOUND)
		{
			ast_list_add(self, expr);
		}
		break;
	}
	case KBETWEEN:
	{
		// NOT BETWEEN
		if (! expr->integer)
			break;

		auto x = expr->r->l;
		auto y = expr->r->r;

		// expr >= x AND expr <= y
		auto gte = ast(KGTE);
		gte->l = expr->l;
		gte->r = x;

		auto lte = ast(KLTE);
		lte->l = expr->l;
		lte->r = y;

		auto and = ast(KAND);
		and->l = gte;
		and->r = lte;
		ops_extract(self, and);
	}
	default:
		break;
	}
}
