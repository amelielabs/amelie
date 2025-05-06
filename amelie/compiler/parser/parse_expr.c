
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static const uint8_t priority_value = 99;

static uint8_t
priority_map[KEYWORD_MAX] =
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
	[KIS]                      = 3,
	[KLIKE]                    = 3,
	// 4
	[KGTE]                     = 4,
	[KLTE]                     = 4,
	['>']                      = 4,
	['<']                      = 4,
	[KBETWEEN]                 = 4,
	[KIN]                      = 4,
	[KAT]                      = 4,
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
	['~']                      = 11,
	// 12
	['[']                      = 12,
	['.']                      = 12,
	[KMETHOD]                  = 12,
	[KARROW]                   = 12,
	// values (priority is not used)
	//
	['(']                      = priority_value,
	['{']                      = priority_value,
	[KCASE]                    = priority_value,
	[KEXISTS]                  = priority_value,
	[KRANDOM]                  = priority_value,
	[KREPLACE]                 = priority_value,
	[KSET]                     = priority_value,
	[KUNSET]                   = priority_value,
	[KEXTRACT]                 = priority_value,
	[KCAST]                    = priority_value,
	[KSELECT]                  = priority_value,
	[KCOUNT]                   = priority_value,
	[KSELF]                    = priority_value,
	[KSUM]                     = priority_value,
	[KAVG]                     = priority_value,
	[KMIN]                     = priority_value,
	[KMAX]                     = priority_value,
	[KERROR]                   = priority_value,
	[KREAL]                    = priority_value,
	[KINT]                     = priority_value,
	[KSTRING]                  = priority_value,
	[KINTERVAL]                = priority_value,
	[KTIMESTAMP]               = priority_value,
	[KDATE]                    = priority_value,
	[KCURRENT_TIMESTAMP]       = priority_value,
	[KCURRENT_DATE]            = priority_value,
	[KVECTOR]                  = priority_value,
	[KUUID]                    = priority_value,
	[KTRUE]                    = priority_value,
	[KFALSE]                   = priority_value,
	[KNULL]                    = priority_value,
	[KARGID]                   = priority_value,
	[KNAME]                    = priority_value,
	[KNAME_COMPOUND]           = priority_value
};

hot static inline void
expr_pop(Stmt* self, AstStack* ops, AstStack* result)
{
	// move operation to result as op(l, r)
	auto head = ast_pop(ops);
	if (head->id == KNEG || head->id == KNOT || head->id == '~')
	{
		// unary
		head->l = ast_pop(result);
		if (unlikely(head->l == NULL))
			stmt_error(self, head, "bad expression");
	} else
	{
		head->r = ast_pop(result);
		head->l = ast_pop(result);
		if (unlikely(head->r == NULL || head->l == NULL))
			stmt_error(self, head, "bad expression");
	}
	ast_push(result, head);
}

hot static inline void
expr_operator(Stmt* self, AstStack* ops, AstStack* result, Ast* op, int prio)
{
	// process last operation if it has lower or equal priority
	//
	// move operation to the result stack
	//
	auto head = ast_head(ops);
	if (head && prio <= head->priority)
		expr_pop(self, ops, result);
	op->priority = prio;
	ast_push(ops, op);
}

hot static inline bool
expr_is_constable(Ast* self)
{
	switch (self->id) {
	// consts
	case KNULL:
	case KREAL:
	case KINT:
	case KSTRING:
	case KTRUE:
	case KFALSE:
		return true;
	case KNEG:
		if (self->l->id == KINT || self->l->id == KREAL)
			return true;
		break;
	case KUUID:
		return true;
	// time-related consts
	case KINTERVAL:
	case KTIMESTAMP:
	case KDATE:
	case KCURRENT_TIMESTAMP:
	case KCURRENT_DATE:
		return true;
	// nested
	case '{':
	case KARRAY:
		return ast_args_of(self->l)->constable;
	}
	return false;
}

Ast*
parse_expr_args(Stmt* self, Expr* expr, int endtoken, bool obj_separator)
{
	bool constable = true;
	int  count     = 0;
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

		// check if the argument can be encoded during compilation
		if (! expr_is_constable(ast))
			constable = false;

		// :
		if (obj_separator && (count % 2) != 0)
		{
			stmt_expect(self, ':');
			continue;
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		stmt_expect(self, endtoken);
		break;
	}

done:;
	// args(list_head, NULL)
	auto args = ast_args_allocate();
	args->ast.l       = expr_head;
	args->ast.r       = NULL;
	args->ast.integer = count;
	args->constable   = constable;
	return &args->ast;
}

static Ast*
expr_func(Stmt* self, Expr* expr, Ast* path, bool with_args)
{
	// [schema.]function_name[(expr, ...)]

	// read schema/name
	Str schema;
	Str name;
	if (! parse_target_path(path, &schema, &name))
		stmt_error(self, path, "bad function call");

	// find and call function
	auto fn = function_mgr_find(self->parser->function_mgr, &schema, &name);
	if (! fn)
		stmt_error(self, path, "function not found");

	auto func = ast_func_allocate();
	func->fn = fn;
	if (with_args)
		func->ast.r = parse_expr_args(self, expr, ')', false);
	return &func->ast;
}

static inline Ast*
expr_aggregate(Stmt* self, Expr* expr, Ast* function)
{
	if (unlikely(!expr || !expr->aggs))
		stmt_error(self, function, "unexpected aggregate function usage");

	// function (expr)
	// (
	stmt_expect(self, '(');

	// [DISTINCT]
	auto distinct = stmt_if(self, KDISTINCT);
	if (distinct && function->id != KCOUNT)
		stmt_error(self, distinct, "is not supported");

	// expr
	Ast* arg  = NULL;
	auto star = stmt_if(self, '*');
	if (star)
	{
		// count(*)
		if (function->id == KCOUNT)
		{
			if (distinct)
				stmt_error(self, star, "* cannot be used with DISTINCT");
			arg = ast(KINT);
			arg->integer = 1;
		} else {
			stmt_error(self, star, "* is not supported by this aggregate function");
		}
	} else {
		arg = parse_expr(self, NULL);
	}

	// )
	stmt_expect(self, ')');

	// create aggregate ast node
	auto agg = ast_agg_allocate(function, expr->aggs->count, arg, NULL, expr->as);
	agg->distinct = distinct;
	ast_list_add(expr->aggs, &agg->ast);
	return &agg->ast;
}

static inline Ast*
expr_lambda(Stmt* self, Ast* seed, Expr* expr)
{
	if (unlikely(!expr || !expr->aggs))
		stmt_error(self, seed, "unexpected lambda usage");

	// create aggregate ast node
	auto agg = ast_agg_allocate(NULL, expr->aggs->count, NULL, seed, expr->as);
	ast_list_add(expr->aggs, &agg->ast);

	// process lambda expression using different context
	Expr ctx;
	expr_init(&ctx);
	ctx.lambda  = &agg->ast;
	ctx.targets = expr->targets;
	agg->expr = parse_expr(self, &ctx);
	return &agg->ast;
}

static Ast*
expr_case(Stmt* self, Expr* expr)
{
	auto ast = ast_case_allocate();
	// CASE [expr] [WHEN expr THEN expr]
	//             [ELSE expr]
	// END
	for (;;)
	{
		// WHEN expr THEN expr
		auto when = stmt_if(self, KWHEN);
		if (when)
		{
			// expr
			when->l = parse_expr(self, expr);

			// THEN
			stmt_expect(self, KTHEN);

			// expr
			when->r = parse_expr(self, expr);
			ast_list_add(&ast->when, when);
			continue;
		}

		// ELSE expr
		auto _else = stmt_if(self, KELSE);
		if (_else)
		{
			if (ast->expr_else)
				stmt_error(self, _else, "ELSE is redefined");
			ast->expr_else = parse_expr(self, expr);
			continue;
		}

		// END
		auto end = stmt_if(self, KEND);
		if (end)
		{
			if (!ast->expr_else && !ast->when.count)
				stmt_error(self, end, "WHEN or ELSE expected");
			break;
		}

		// CASE expr
		if (ast->expr || ast->expr_else || ast->when.count > 0)
			stmt_error(self, NULL, "WHEN, ELSE or END expected");
		ast->expr = parse_expr(self, expr);
	}
	return &ast->ast;
}

hot static inline Ast*
expr_extract(Stmt* self, Expr* expr, Ast* value)
{
	// replace as call to public.extract(field, expr)

	// make explicitly lower case
	str_set(&value->string, "extract", 7);

	// (
	stmt_expect(self, '(');
	value->id = KNAME;
	value = expr_func(self, expr, value, false);

	// field
	auto field = stmt_next_shadow(self);
	if (field->id != KNAME && field->id != KSTRING)
		stmt_error(self, field, "name or string expected");
	field->id = KSTRING;

	// FROM
	stmt_expect(self, KFROM);

	// expr
	auto time = parse_expr(self, expr);
	field->next = time;

	// )
	stmt_expect(self, ')');

	// args(list_head, NULL)
	value->r = &ast_args_allocate()->ast;
	value->r->l       = field;
	value->r->integer = 2;
	return value;
}

hot static inline Ast*
expr_cast(Stmt* self, Expr* expr)
{
	// CAST (
	stmt_expect(self, '(');

	// expr (prepare args for call)
	auto args = &ast_args_allocate()->ast;
	args->l       = parse_expr(self, expr);
	args->integer = 1;

	// AS
	stmt_expect(self, KAS);

	// type
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME)
		stmt_error(self, name, "type name expected");

	int type_size;
	int type = type_read(&name->string, &type_size);
	if (type == -1)
		stmt_error(self, name, "unsupported data type");
	switch (type) {
	case TYPE_BOOL:
		str_set(&name->string, "bool", 4);
		break;
	case TYPE_INT:
		str_set(&name->string, "int", 3);
		break;
	case TYPE_DOUBLE:
		str_set(&name->string, "double", 6);
		break;
	case TYPE_STRING:
		str_set(&name->string, "string", 6);
		break;
	case TYPE_JSON:
		str_set(&name->string, "json", 4);
		break;
	case TYPE_DATE:
		str_set(&name->string, "date", 4);
		break;
	case TYPE_TIMESTAMP:
		str_set(&name->string, "timestamp", 9);
		break;
	case TYPE_INTERVAL:
		str_set(&name->string, "interval", 8);
		break;
	case TYPE_VECTOR:
		str_set(&name->string, "vector", 6);
		break;
	case TYPE_UUID:
		str_set(&name->string, "uuid", 4);
		break;
	// TYPE_NULL:
	default:
		abort();
		break;
	}

	// )
	stmt_expect(self, ')');

	auto call = expr_func(self, expr, name, false);
	call->r = args;
	return call;
}

hot static inline Ast*
expr_value(Stmt* self, Expr* expr, Ast* value)
{
	switch (value->id) {
	// bracket
	case '(':
		// ()
		value = parse_expr(self, expr);
		stmt_expect(self, ')');
		break;

	// case
	case KCASE:
		value = expr_case(self, expr);
		break;

	// exists
	case KEXISTS:
	{
		stmt_expect(self, '(');
		auto select = stmt_expect(self, KSELECT);
		if (!expr || !expr->select)
			stmt_error(self, select, "unexpected subquery");
		assert(expr->targets);
		value->r = &parse_select(self, expr->targets, true)->ast;
		stmt_expect(self, ')');
		break;
	}

	// object
	case '{':
		// { [expr: expr, ... ] }
		value->l = parse_expr_args(self, expr, '}', true);
		break;

	// functions (keyword conflicts)
	case KSET:
	case KUNSET:
	case KRANDOM:
	case KREPLACE:
	case KERROR:
	{
		stmt_expect(self, '(');
		value->id = KNAME;
		value = expr_func(self, expr, value, true);
		break;
	}

	// EXTRACT (field FROM expr)
	case KEXTRACT:
		value = expr_extract(self, expr, value);
		break;

	// CAST (expr AS type)
	case KCAST:
		value = expr_cast(self, expr);
		break;

	// sub-query
	case KSELECT:
	{
		if (!expr || !expr->select)
			stmt_error(self, value, "unexpected subquery");
		assert(expr->targets);
		auto select = parse_select(self, expr->targets, true);
		select->ast.pos_start = value->pos_start;
		select->ast.pos_end   = value->pos_end;
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

	// lambda
	case KSELF:
		if (!expr || !expr->lambda)
			stmt_error(self, value, "unexpected SELF usage without lambda context");
		value->r = expr->lambda;
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
		if (stmt_if(self, '('))
		{
			value->id = KNAME;
			value = expr_func(self, expr, value, true);
			break;
		}
		// interval 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string    = spec->string;
		value->pos_start = spec->pos_start;
		value->pos_end   = spec->pos_end;
		break;
	}
	case KTIMESTAMP:
	{
		// ()
		if (stmt_if(self, '('))
		{
			value->id = KNAME;
			value = expr_func(self, expr, value, true);
			break;
		}
		// timestamp 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string    = spec->string;
		value->pos_start = spec->pos_start;
		value->pos_end   = spec->pos_end;
		break;
	}
	case KDATE:
	{
		// ()
		if (stmt_if(self, '('))
		{
			value->id = KNAME;
			value = expr_func(self, expr, value, true);
			break;
		}
		// date 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string    = spec->string;
		value->pos_start = spec->pos_start;
		value->pos_end   = spec->pos_end;
		break;
	}
	case KCURRENT_TIMESTAMP:
	case KCURRENT_DATE:
		break;

	case KVECTOR:
		// vector [value, ...]
		value->integer = code_data_offset(&self->parser->program->code_data);
		parse_vector(self, &self->parser->program->code_data.data);
		break;

	// uuid
	case KUUID:
	{
		// uuid()
		if (stmt_if(self, '('))
		{
			value->id = KNAME;
			value = expr_func(self, expr, value, true);
			break;
		}
		// uuid 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string    = spec->string;
		value->pos_start = spec->pos_start;
		value->pos_end   = spec->pos_end;
		break;
	}

	// request argument
	case KARGID:
		break;

	// name
	case KNAME:
	{
		// function(expr, ...)
		if (stmt_if(self,'('))
			value = expr_func(self, expr, value, true);
		break;
	}

	// name.path
	// name.path.*
	case KNAME_COMPOUND:
	{
		// function(expr, ...)
		if (stmt_if(self,'('))
			value = expr_func(self, expr, value, true);
		break;
	}

	default:
		abort();
		break;
	}

	return value;
}

hot static inline Ast*
expr_value_between(Stmt* self)
{
	auto value = stmt_next(self);
	switch (value->id) {
	case KREAL:
	case KINT:
	case KSTRING:
		break;
	case KINTERVAL:
	{
		// interval 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string = spec->string;
		break;
	}
	case KTIMESTAMP:
	{
		// timestamp 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string = spec->string;
		break;
	}
	case KDATE:
	{
		// date 'spec'
		auto spec = stmt_expect(self, KSTRING);
		value->string = spec->string;
		break;
	}
	default:
		stmt_error(self, value, "const value expected");
		break;
	}
	return value;
}

hot static inline bool
parse_unary(Stmt*     self, Expr* expr,
            AstStack* ops,
            AstStack* result,
            Ast*      ast)
{
	switch (ast->id) {
	case '[':
		// [ [expr, ...] ]
		ast->id = KARRAY;
		ast->l  = parse_expr_args(self, expr, ']', false);
		ast_push(result, ast);
		break;
	case KNOT:
		// not expr
		expr_operator(self, ops, result, ast, 11);
		break;
	case '-':
		// - expr
		ast->id = KNEG;
		expr_operator(self, ops, result, ast, 11);
		break;
	case '~':
		// ~ expr
		expr_operator(self, ops, result, ast, 11);
		break;
	default:
		stmt_error(self, ast, "bad expression");
		break;
	}
	return false;
}

hot static inline bool
parse_op(Stmt*     self, Expr* expr,
         AstStack* ops,
         AstStack* result,
         Ast*      ast,
         int       priority)
{
	auto not = ast->id == KNOT;
	if (not)
	{
		// expr NOT BETWEEN
		// expr NOT IN
		// expr NOT LIKE
		if (stmt_if(self, KBETWEEN))
			ast->id = KBETWEEN;
		else
		if (stmt_if(self, KIN))
			ast->id = KIN;
		else
		if (stmt_if(self, KLIKE))
			ast->id = KLIKE;
		else
			stmt_error(self, ast, "NOT 'IN or BETWEEN or LIKE' expected");
	}

	// operator
	expr_operator(self, ops, result, ast, priority);

	bool unary = false;
	switch (ast->id) {
	case '=':
	case KNEQU:
	case KGTE:
	case KLTE:
	case '>':
	case '<':
	{
		// expr operator [ANY|ALL] (expr)
		auto r = stmt_next(self);
		if (r->id == KANY || r->id == KALL)
		{
			//     . ANY|ALL .
			//  expr         op .
			//                  expr
			int op = ast->id;
			ast->id = r->id;
			r->id = op;
			stmt_expect(self, '(');
			r->r = parse_expr(self, expr);
			ast_push(result, r);
			stmt_expect(self, ')');
		} else
		{
			stmt_push(self, r);
		}
		unary = true;
		break;
	}
	case KBETWEEN:
	{
		// expr [NOT] BETWEEN x AND y
		ast->integer = !not;
		auto x = expr_value_between(self);
		auto r = stmt_expect(self, KAND);
		auto y = expr_value_between(self);
		//
		//    . BETWEEN .
		// expr       . AND .
		//            x     y
		r->l = x;
		r->r = y;
		ast_push(result, r);
		break;
	}
	case KIS:
	{
		// expr IS [NOT] expr
		auto not = stmt_if(self, KNOT);
		ast->integer = !not;
		// right expression must be null
		break;
	}
	case KLIKE:
	{
		// expr [NOT] LIKE pattern
		ast->integer = !not;
		auto r = parse_expr(self, expr);
		ast_push(result, r);
		break;
	}
	case KIN:
	{
		// expr [NOT] IN (value, ...)
		ast->integer = !not;
		stmt_expect(self, '(');
		auto r = parse_expr_args(self, expr, ')', false);
		ast_push(result, r);
		break;
	}
	case KAT:
	{
		// expr AT TIMEZONE expr
		stmt_expect(self, KTIMEZONE);
		auto r = parse_expr(self, expr);
		ast_push(result, r);
		break;
	}
	case KARROW:
	{
		// expr -> expr
		auto r = expr_lambda(self, ast_head(result), expr);
		ast_push(result, r);
		break;
	}
	case KMETHOD:
	{
		// expr :: path [(call, ...)]
		auto r = stmt_next_shadow(self);
		if (r->id == KNAME ||
			r->id == KNAME_COMPOUND)
		{
			// function[(expr, ...)]
			auto with_args = stmt_if(self, '(') != NULL;
			r = expr_func(self, expr, r, with_args);
		} else {
			stmt_error(self, r, "function name expected");
		}
		ast_push(result, r);
		break;
	}
	case '[':
	{
		// expr[idx]
		auto r = parse_expr(self, expr);
		ast_push(result, r);
		stmt_expect(self, ']');
		break;
	}
	case '.':
	{
		// expr.'key' (handle as expr['key'])
		// expr.name
		// expr.name.path
		auto r = stmt_next_shadow(self);
		if (r->id == KSTRING)
			ast->id = '[';
		else
		if (r->id == KNAME || r->id == KNAME_COMPOUND)
			r->id = KSTRING;
		else
			stmt_error(self, r, "name expected");
		ast_push(result, r);
		break;
	}
	default:
		// next operation should be either const or unary
		unary = true;
		break;
	}
	return unary;
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
			// unary/object (handle as value) or operator
			if (unary)
				unary = parse_unary(self, expr, &ops, &result, ast);
			else
				unary = parse_op(self, expr, &ops, &result, ast, priority);
		}
	}

	while (ast_head(&ops))
		expr_pop(self, &ops, &result);

	// only one result
	if (! result.list)
		stmt_error(self, NULL, "bad expression");

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
