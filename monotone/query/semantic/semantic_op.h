#pragma once

//
// monotone
//
// SQL OLTP database
//

hot static inline void
semantic_op(AstList* ops, Ast* expr)
{
	if (expr == NULL)
		return;
	switch (expr->id) {
	case KAND:
		semantic_op(ops, expr->l);
		semantic_op(ops, expr->r);
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
