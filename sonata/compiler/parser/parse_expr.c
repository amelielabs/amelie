
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

static const uint8_t priority_value = 99;

static uint8_t
priority_map[UINT8_MAX] =
{
	// operators:
	//
	// 1
	[KOR]                      = 1,
	// 2
	[KAND]                     = 2,
	// 3
	['=']                      = 3,
	[KNEQU]                    = 3,
	// 4
	[KGTE]                     = 4,
	[KLTE]                     = 4,
	['>']                      = 4,
	['<']                      = 4,
	// 5
	['|']                      = 5,
	// 6
	['^']                      = 6,
	// 7
	['&']                      = 7,
	// 8
	[KSHL]                     = 8,
	[KSHR]                     = 8,
	// 9
	['+']                      = 9,
	['-']                      = 9,
	[KNOT]                     = 9,
	[KCAT]                     = 9,
	// 10
	['*']                      = 10,
	['/']                      = 10,
	['%']                      = 10,
	// 11 (reserved for unary)
	// 12
	[KMETHOD]                  = 12,
	['[']                      = 12,
	['.']                      = 12,
	// values (priority is not used)
	//
	['(']                      = priority_value,
	['{']                      = priority_value,
	['@']                      = priority_value,
	[KSET]                     = priority_value,
	[KUNSET]                   = priority_value,
	[KSELECT]                  = priority_value,
	[KCOUNT]                   = priority_value,
	[KSUM]                     = priority_value,
	[KAVG]                     = priority_value,
	[KMIN]                     = priority_value,
	[KMAX]                     = priority_value,
	[KREAL]                    = priority_value,
	[KINT]                     = priority_value,
	[KSTRING]                  = priority_value,
	[KINTERVAL]                = priority_value,
	[KTRUE]                    = priority_value,
	[KFALSE]                   = priority_value,
	[KNULL]                    = priority_value,
	[KARGUMENT]                = priority_value,
	[KNAME]                    = priority_value,
	[KNAME_COMPOUND]           = priority_value,
	[KNAME_COMPOUND_STAR]      = priority_value,
	[KNAME_COMPOUND_STAR_STAR] = priority_value,
	[KSTAR_STAR]               = priority_value
};

hot static inline void
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

hot static inline void
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
expr_args(Stmt* self, Expr* expr, int endtoken, bool map_separator)
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
	// args(list_head, NULL)
	auto args = ast(KARGS);
	args->l       = expr_head;
	args->r       = NULL;
	args->integer = count;
	return args;
}

static Ast*
expr_call(Stmt* self, Expr* expr, Ast* path, bool with_args)
{
	// [schema.]function_name[(expr, ...)]

	// read schema/name
	Str schema;
	Str name;
	if (! parse_target_path(path, &schema, &name))
		error("%.*s(): bad function call", str_size(&path->string),
		      str_of(&path->string));

	// find and call function
	auto func = function_mgr_find(self->function_mgr, &schema, &name);
	if (! func)
		error("%.*s(): function not found", str_size(&path->string),
		      str_of(&path->string));

	auto call = ast_call_allocate();
	call->fn = func;
	call->ast.l = path;
	call->ast.r = NULL;
	if (with_args)
		call->ast.r = expr_args(self, expr, ')', false);
	return &call->ast;
}

static inline Ast*
expr_aggregate(Stmt* self, Expr* expr, Ast* function)
{
	if (unlikely(expr == NULL || !expr->aggs))
		error("unexpected aggregate function usage");

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
	case KMIN:
		id = AGGR_MIN;
		break;
	case KMAX:
		id = AGGR_MAX;
		break;
	default:
		abort();
		break;
	}

	// create aggregate ast node
	auto aggr = ast_aggr_allocate(id, expr->aggs->count, arg);
	ast_list_add(expr->aggs, &aggr->ast);
	return &aggr->ast;
}

static inline Ast*
expr_lambda(Stmt* self, Expr* expr, Ast* name)
{
	if (unlikely(expr == NULL || !expr->aggs))
		error("unexpected lambda usage");

	// ensure lambda is not redefined
	auto node = expr->aggs->list;
	for (; node; node = node->next)
	{
		auto aggr = ast_aggr_of(node->ast);
		if (! aggr->name)
			continue;
		if (str_compare(&aggr->name->string, &name->string))
			error("lambda <%.*s> redefined", str_size(&name->string),
			      str_of(&name->string));
	}

	// name => expr
	auto arg = parse_expr(self, expr);

	// create aggregate ast node
	auto aggr = ast_aggr_allocate(AGGR_LAMBDA, expr->aggs->count, arg);
	ast_list_add(expr->aggs, &aggr->ast);
	aggr->name = name;
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
		value->l = expr_args(self, expr, '}', true);
		break;

	// builtin functions
	case KSET:
	case KUNSET:
	{
		if (! stmt_if(self,'('))
			error("%.*s<(> expected", str_size(&value->string),
			      str_of(&value->string));
		value->id = KNAME;
		value = expr_call(self, expr, value, true);
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
	case KMIN:
	case KMAX:
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

	// time-related values
	case KINTERVAL:
	{
		// interval()
		if (stmt_if(self,'('))
		{
			value->id = KNAME;
			value = expr_call(self, expr, value, true);
			break;
		}
		// interval 'spec'
		auto iv = stmt_if(self, KSTRING);
		if (! iv)
			error("INTERVAL <string> expected");
		interval_init(&value->interval);
		interval_read(&value->interval, &iv->string);
		break;
	}

	// request argument
	case KARGUMENT:
		break;

	// @, **
	case '@':
	case KSTAR_STAR:
		break;

	// name
	// name.path
	// name.path.*
	case KNAME:
	case KNAME_COMPOUND:
	{
		// name => lambda expr
		if (stmt_if(self, KARROW))
		{
			value = expr_lambda(self, expr, value);
			break;
		}

		// function(expr, ...)
		if (stmt_if(self,'('))
			value = expr_call(self, expr, value, true);
		break;
	}
	case KNAME_COMPOUND_STAR:
	case KNAME_COMPOUND_STAR_STAR:
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
            AstStack* result,
            Ast*      ast,
            int       priority)
{
	if (ast->id == '[') 
	{
		// [ [expr, ...] ]
		ast->id = KARRAY;
		ast->l  = expr_args(self, expr, ']', false);
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

				if (ast->id == KMETHOD)
				{
					// expr :: path [(call, ...)]
					// expr -> path [(call, ...)]
					auto r = stmt_next_shadow(self);
					if (r->id == KNAME ||
					    r->id == KNAME_COMPOUND)
					{
						// function[(expr, ...)]
						auto with_args = stmt_if(self, '(') != NULL;
						r = expr_call(self, expr, r, with_args);
					} else {
						error("bad '::' or '->' expression");
					}
					ast_push(&result, r);
					unary = false;
				} else
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
					auto r = stmt_next_shadow(self);
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
	if (! result.list)
		error("bad expression");

	if (likely(result.list->next == NULL))
		return result.list;

	// use head as a result
	auto head = result.list;
	head->next->prev = NULL;
	head->next = NULL;

	// pushback the rest
	auto prev = result.list_tail;
	while (prev)
	{
		auto prev_prev = prev->prev;
		stmt_push(self, ast_pop(&result));
		prev = prev_prev;
	}
	return head;
}
