#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline void
semantic_get_op(Ast* expr, AstList* ops)
{
	switch (expr->id) {
	case KAND:
		semantic_get_op(expr->l, ops);
		semantic_get_op(expr->r, ops);
		break;
	case KOR:
		ast_list_init(ops);
		break;
	case '=':
	case KGTE:
	case KLTE:
	case '>':
	case '<':
		if (expr->l->id == KNAME || expr->l->id == KNAME_COMPOUND)
		{
			if (expr->r->id == KINT || expr->r->id == KSTRING)
				ast_list_add(ops, expr);
		} else
		if (expr->r->id == KNAME || expr->r->id == KNAME_COMPOUND)
		{
			if (expr->l->id == KINT || expr->l->id == KSTRING)
				ast_list_add(ops, expr);
		}
		break;
	}
}
