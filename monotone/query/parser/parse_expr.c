
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static const uint8_t priority_value = 99;

static uint8_t
priority_map[UINT8_MAX] =
{
	// operators:
	//
	// 1
	[KOR]                 = 1,
	// 2
	[KAND]                = 2,
	// 3
	['=']                 = 3,
	[KNEQU]               = 3,
	// 4
	[KGTE]                = 4,
	[KLTE]                = 4,
	['>']                 = 4,
	['<']                 = 4,
	// 5
	['|']                 = 5,
	// 6
	['^']                 = 6,
	// 7
	['&']                 = 7,
	// 8
	[KSHL]                = 8,
	[KSHR]                = 8,
	// 9
	['+']                 = 9,
	['-']                 = 9,
	[KNOT]                = 9,
	[KCAT]                = 9,
	// 10
	['*']                 = 10,
	['/']                 = 10,
	['%']                 = 10,
	// 11 (reserved for unary)
	// 12
	['[']                 = 12,
	['.']                 = 12,
	// values (priority is not used)
	//
	['(']                 = priority_value,
	['{']                 = priority_value,
	[KSET]                = priority_value,
	[KUNSET]              = priority_value,
	[KTSTRING]            = priority_value,
	[KJSON]               = priority_value,
	[KSELECT]             = priority_value,
	[KCOUNT]              = priority_value,
	[KSUM]                = priority_value,
	[KAVG]                = priority_value,
	[KREAL]               = priority_value,
	[KINT]                = priority_value,
	[KSTRING]             = priority_value,
	[KTRUE]               = priority_value,
	[KFALSE]              = priority_value,
	[KNULL]               = priority_value,
	[KARGUMENT]           = priority_value,
	[KARGUMENT_NAME]      = priority_value,
	[KNAME]               = priority_value,
	[KNAME_COMPOUND]      = priority_value,
	[KNAME_COMPOUND_STAR] = priority_value
};

static inline void
expr_pop(AstStack* ops, AstStack* result)
{
	// move operation to result as op(l, r)
	auto head = ast_pop(ops);
	if (head->id == KNEG || head->id == KNOT)
	{
		// unary
		head->l = ast_pop(result);
		if (unlikely(head->l == NULL))
			error("bad expression");
	} else
	{
		head->r = ast_pop(result);
		head->l = ast_pop(result);
		if (unlikely(head->r == NULL || head->l == NULL))
			error("bad expression");
	}
	ast_push(result, head);
}

static inline void
expr_operator(AstStack* ops, AstStack* result, Ast* op, int prio)
{
	// process last operation if it has lower or equal priority
	//
	// move operation to the result stack
	//
	auto head = ast_head(ops);
	if (head && prio <= head->priority)
		expr_pop(ops, result);
	op->priority = prio;
	ast_push(ops, op);
}

static Ast*
expr_call(Stmt* self, Expr* expr, int endtoken, bool map_separator)
{
	int  count = 0;
	Ast* expr_head = NULL;
	Ast* expr_prev = NULL;

	if (stmt_if(self, endtoken))
		goto done;

	for (;;)
	{
		// expr
		auto ast = parse_expr(self, expr);
		if (expr_head == NULL)
			expr_head = ast;
		else
			expr_prev->next = ast;
		expr_prev = ast;
		count++;

		// :
		if (map_separator && (count % 2) != 0)
		{
			if (! stmt_if(self, ':'))
				error("{}: {name <:>} expected");
			continue;
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		if (! stmt_if(self, endtoken))
			error("'%c' expected", endtoken);
		break;
	}

done:;
	// call(list_head, NULL)
	auto call = ast(KCALL);
	call->l       = expr_head;
	call->r       = NULL;
	call->integer = count;
	return call;
}

static inline Ast*
expr_aggregate(Stmt* self, Expr* expr, Ast* function)
{
	// function (expr)
	// (
	if (! stmt_if(self, '('))
		error("%.*s<(> expected", str_size(&function->string),
		      str_of(&function->string));
	// expr
	auto arg = parse_expr(self, expr);

	// )
	if (! stmt_if(self, ')'))
		error("%.*s(expr<)> expected", str_size(&function->string),
		      str_of(&function->string));

	// [label]
	auto label = stmt_if(self, KNAME);
	if (label)
	{
		if (ast_aggr_match(expr->aggs_global, &label->string))
			error("<%.*s()> aggregate label '%.*s'redefined",
			      str_size(&function->string),
			      str_of(&function->string),
			      str_size(&label->string),
			      str_of(&label->string));
	}

	// get aggregate type
	int id;
	switch (function->id) {
	case KCOUNT:
		id = AGGR_COUNT;
		break;
	case KSUM:
		id = AGGR_SUM;
		break;
	case KAVG:
		id = AGGR_AVG;
		break;
	default:
		abort();
		break;
	}

	// create aggregate ast node
	int order = expr->aggs_global->count;
	auto aggr = ast_aggr_allocate(function->id, id, order, arg, label);
	if (expr)
	{
		ast_list_add(expr->aggs, &aggr->ast);
		ast_list_add(expr->aggs_global, &aggr->ast);
	}

	return &aggr->ast;
}

hot static inline Ast*
expr_value(Stmt* self, Expr* expr, Ast* value)
{
	switch (value->id) {
	// bracket
	case '(':
		// ()
		value = parse_expr(self, expr);
		if (! stmt_if(self,')'))
			error("(): ')' expected");
		break;

	// map
	case '{':
		// { [expr: expr, ... ] }
		value->l = expr_call(self, expr, '}', true);
		break;

	// builtin functions
	case KSET:
	case KUNSET:
	case KTSTRING:
	case KJSON:
	{
		// function(call, NULL)
		if (! stmt_if(self, '('))
			error("%.*s<(> expected", str_size(&value->string),
			      str_of(&value->string));
		auto name = value;
		value    = ast('(');
		value->l = name;
		value->r = expr_call(self, expr, ')', false);
		break;
	}

	// sub-query
	case KSELECT:
	{
		auto select = parse_select(self);
		value = &select->ast;
		break;
	}

	// aggregates
	case KCOUNT:
	case KSUM:
	case KAVG:
		value = expr_aggregate(self, expr, value);
		break;

	// const
	case KNULL:
	case KREAL:
	case KINT:
	case KSTRING:
	case KTRUE:
	case KFALSE:
		break;

	case KARGUMENT:
	case KARGUMENT_NAME:
		break;

	// name
	case KNAME:
	{
		auto call = stmt_if(self, '(');
		if (call)
		{
			// function(expr, ...)
			call->l = value;
			call->r = expr_call(self, expr, ')', false);
			value = call;
		}
		break;
	}
	case KNAME_COMPOUND:
		break;

	default:
		abort();
		break;
	}

	return value;
}

hot static inline int
parse_unary(Stmt*     self, Expr* expr,
            AstStack* ops,
            AstStack* result, Ast* ast,
            int       priority)
{
	if (ast->id == '[') 
	{
		// [ [expr, ...] ]
		ast->id = KARRAY;
		ast->l  = expr_call(self, expr, ']', false);
		ast_push(result, ast);
		priority = priority_value;
	} else
	if (ast->id == '*')
	{
		// *
		ast->id = KSTAR;
		ast_push(result, ast);
		priority = priority_value;
	} else
	if (ast->id == KNOT)
	{
		// not expr
		expr_operator(ops, result, ast, 11);
	} else
	if (ast->id == '-')
	{
		// - expr
		ast->id = KNEG;
		expr_operator(ops, result, ast, 11);
	} else
	{
		error("bad expression");
	}
	return priority;
}

hot Ast*
parse_expr(Stmt* self, Expr* expr)
{
	AstStack ops;
	ast_stack_init(&ops);

	AstStack result;
	ast_stack_init(&result);

	bool unary = true;
	bool done  = false;
	while (! done)
	{
		auto ast = stmt_next(self);

		int priority = priority_map[ast->id];
		if (priority == 0)
		{
			// unknown token or eof
			done = true;
			stmt_push(self, ast);
		} else
		if (priority == priority_value)
		{
			// value
			auto value = expr_value(self, expr, ast);
			ast_push(&result, value);
			unary = false;
		} else
		{
			if (unary)
			{
				// unary/object (handle as value) or operator
				priority = parse_unary(self, expr, &ops, &result, ast, priority);
				unary = false;
			} else
			{
				if (unlikely(ast->id == KNOT))
					error("bad expression");

				// operator
				expr_operator(&ops, &result, ast, priority);

				if (ast->id == '[') 
				{
					// expr[idx]
					auto r = parse_expr(self, expr);
					ast_push(&result, r);
					if (! stmt_if(self,']'))
						error("[]: ']' expected");
					unary = false;
				} else
				if (ast->id == '.')
				{
					// expr.name
					// expr.path.to
					auto r = stmt_next(self);
					if (r->id == KNAME ||
					    r->id == KNAME_COMPOUND)
						r->id = KSTRING;
					else
						error("bad '.' expression");
					ast_push(&result, r);
					unary = false;
				} else {
					unary = true;
				}
			}
		}
	}

	while (ast_head(&ops))
		expr_pop(&ops, &result);

	// only one result
	if (!result.list || result.list->next)
		error("bad expression");

	return ast_pop(&result);
}
