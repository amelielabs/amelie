
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
#include <amelie_compiler.h>

hot int
emit_string(Compiler* self, Str* string, bool escape)
{
	int offset;
	if (escape)
		offset = code_data_add_string_unescape(self->code_data, string);
	else
		offset = code_data_add_string(self->code_data, string);
	return op2(self, CSTRING, rpin(self, TYPE_STRING), offset);
}

hot static inline int
emit_json(Compiler* self, Targets* targets, Ast* ast)
{
	assert(ast->l->id == KARGS);
	auto args = ast_args_of(ast->l);

	// encode json if possible
	if (args->constable)
	{
		int offset = code_data_offset(self->code_data);
		ast_encode(ast, &self->parser.lex, self->parser.local, &self->code_data->data);
		return op2(self, CJSON, rpin(self, TYPE_JSON), offset);
	}

	// push arguments
	auto current = args->ast.l;
	while (current)
	{
		int r = emit_expr(self, targets, current);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// op
	int op;
	if (ast->id == KARRAY)
		op = CJSON_ARRAY;
	else
		op = CJSON_OBJ;
	return op2(self, op, rpin(self, TYPE_JSON), args->ast.integer);
}

hot static inline int
emit_column(Compiler* self, Target* target, Ast* ast,
            Column*   column,
            Str*      name,
            bool      excluded)
{
	// target.column_name
	bool column_conflict = false;
	if (! column)
	{
		auto cte = target->from_cte;
		if (target->type == TARGET_CTE && cte->cte->args.count > 0)
		{
			// find column in the CTE arguments list, redirect to the CTE statement
			auto arg = columns_find(&cte->cte->args, name);
			if (arg)
				column = columns_find_by(target->from_columns, arg->order);
		} else
		{
			// find unique column name in the target
			column = columns_find_noconflict(target->from_columns, name, &column_conflict);
		}

		if (!column && target->type != TARGET_FUNCTION)
		{
			if (column_conflict)
				stmt_error(self->current, ast,
				           "column %.*s.%.*s is ambiguous",
				           str_size(&target->name), str_of(&target->name),
				           str_size(name), str_of(name));
			else
				stmt_error(self->current, ast, "column %.*s.%.*s not found",
				           str_size(&target->name), str_of(&target->name),
				           str_size(name), str_of(name));
		}
	}

	// read excluded column
	if (unlikely(excluded))
		return op3(self, CEXCLUDED, rpin(self, column->type),
		           target->id, column->order);

	// generate cursor read based on the target
	int r;
	if (target->type == TARGET_TABLE)
	{
		int op;
		switch (column->type) {
		case TYPE_BOOL:
			op = CTABLE_READB;
			break;
		case TYPE_INT:
		{
			switch (column->type_size) {
			case 1: op = CTABLE_READI8;
				break;
			case 2: op = CTABLE_READI16;
				break;
			case 4: op = CTABLE_READI32;
				break;
			case 8: op = CTABLE_READI64;
				break;
			default:
				abort();
				break;
			}
			break;
		}
		case TYPE_DOUBLE:
		{
			switch (column->type_size) {
			case 4: op = CTABLE_READF32;
				break;
			case 8: op = CTABLE_READF64;
				break;
			default:
				abort();
				break;
			}
			break;
		}
		case TYPE_STRING:
			op = CTABLE_READS;
			break;
		case TYPE_JSON:
			op = CTABLE_READJ;
			break;
		case TYPE_TIMESTAMP:
			op = CTABLE_READT;
			break;
		case TYPE_INTERVAL:
			op = CTABLE_READL;
			break;
		case TYPE_DATE:
			op = CTABLE_READD;
			break;
		case TYPE_VECTOR:
			op = CTABLE_READV;
			break;
		case TYPE_UUID:
			op = CTABLE_READU;
			break;
		default:
			abort();
			break;
		}
		r = op3(self, op, rpin(self, column->type),
		        target->id, column->order);
	} else
	if (target->type == TARGET_EXPR ||
	    target->type == TARGET_FUNCTION)
	{
		assert(target->r != -1);
		r = op2(self, CJSON_READ, rpin(self, TYPE_JSON), target->id);
		if (! column)
		{
			// handle as {}.column for json target
			auto rstring = emit_string(self, name, false);
			auto rdot = cast_operator(self, ast, OP_DOT, r, rstring);
			runpin(self, r);
			r = rdot;
		}
	} else
	{
		assert(target->r != -1);
		r = op3(self, CSTORE_READ, rpin(self, column->type),
		        target->id, column->order);
	}
	return r;
}

hot static inline int
emit_name(Compiler* self, Targets* targets, Ast* ast)
{
	// SELECT name
	auto name = &ast->string;

	// find variable by name
	auto var = vars_find(&self->current->scope->vars, name);
	if (var)
	{
		assert(var->r != -1);
		return op2(self, CREF, rpin(self, var->type), var->r);
	}

	while (targets && targets_empty(targets))
		targets = targets->outer;
	if (! targets)
		stmt_error(self->current, ast, "column not found");

	// SELECT name FROM
	if (targets_is_join(targets))
		stmt_error(self->current, ast, "column requires explicit target name");

	auto target = targets_outer(targets);

	// SELECT name FROM GROUP BY name
	auto group_by_column = NULL;
	if (target->type == TARGET_GROUP_BY)
	{
		auto group = ast_group_resolve_column(target->from_group_by, name);
		if (! group)
			stmt_error(self->current, ast, "column %.*s must appear in the GROUP BY clause "
			           "or be used by an aggregate function",
			           str_size(name), str_of(name));
		group_by_column = group->column;
	}

	return emit_column(self, target, ast, group_by_column, name, false);
}

hot static inline int
emit_name_compound(Compiler* self, Targets* targets, Ast* ast)
{
	// target.column[.path]
	// column.path
	Str name;
	str_split(&ast->string, &name, '.');

	Str path;
	str_init(&path);
	str_set_str(&path, &ast->string);

	// find variable by name
	auto var = vars_find(&self->current->scope->vars, &name);
	if (var)
	{
		assert(var->r != -1);
		auto r = op2(self, CREF, rpin(self, var->type), var->r);
		if (str_empty(&path))
			return r;

		// ensure variable is a json
		if (var->type != TYPE_JSON)
			stmt_error(self->current, ast, "variable is not a json");

		// var.path
		str_advance(&path, str_size(&name) + 1);
		int rstring = emit_string(self, &path, false);
		return cast_operator(self, ast, OP_DOT, r, rstring);
	}

	// check if the first path is a target name
	Target* target = NULL;
	Column* group_by_column = NULL;

	// excluded.column
	bool excluded = false;
	if (unlikely(str_is_case(&name, "excluded", 8)))
	{
		// exclude prefix
		str_advance(&path, 9);
		if (str_split(&path, &name, '.'))
			str_advance(&path, str_size(&name) + 1);
		else
			str_advance(&path, str_size(&name));
		excluded = true;
		if (targets == NULL ||
		    targets->count != 1 ||
		    targets_outer(targets)->type != TARGET_TABLE)
			stmt_error(self->current, ast, "cannot be used outside of INSERT ON CONFLICT DO UPDATE statement");
		target = targets_outer(targets);
	} else
	{
		// find target
		if (targets && targets->count == 1)
			target = targets_outer(targets);

		if (target && target->type == TARGET_GROUP_BY)
		{
			// SELECT [target.]name[.path] FROM GROUP BY [target.]name[.path]

			// instead of searching for targets, use path to match the group by
			// expression and use its column

			// group by name can be a prefix of the path, in case of json columns
			auto group = ast_group_resolve_column_prefix(target->from_group_by, &path);
			if (! group)
				stmt_error(self->current, ast, "column %.*s must appear in the GROUP BY clause "
				           "or be used by an aggregate function",
				           str_size(&path), str_of(&path));

			// use group by name as a prefix for the path
			str_advance(&path, str_size(&group->expr->string));
			if (!str_empty(&path) && *path.pos == '.')
				str_advance(&path, 1);

			group_by_column = group->column;
		} else
		{
			// find target by name or use as a column with json path
			Target* match;
			if (target && str_compare(&target->name, &name))
				match = target;
			else
				match = targets_match_outer(targets, &name);
			if (match)
			{
				// target.column
				target = match;

				// exclude target name from the path
				str_advance(&path, str_size(&name) + 1);

				if (str_split(&path, &name, '.'))
					str_advance(&path, str_size(&name) + 1);
				else
					str_advance(&path, str_size(&name));
			} else
			{
				// exclude column name from the path
				str_advance(&path, str_size(&name) + 1);
			}
		}

		if (! target)
			stmt_error(self->current, ast, "target not found");
	}

	// column[.path]
	auto rcolumn = emit_column(self, target, ast, group_by_column, &name, excluded);

	if (str_empty(&path))
		return rcolumn;

	// ensure column is a json
	if (rtype(self, rcolumn) != TYPE_JSON)
		stmt_error(self->current, ast, "column is not a json");

	// column.path
	int rstring = emit_string(self, &path, false);
	return cast_operator(self, ast, OP_DOT, rcolumn, rstring);
}

hot static inline int
emit_aggregate(Compiler* self, Targets* targets, Ast* ast)
{
	// SELECT agg GROUP BY
	// SELECT GROUP BY HAVING agg

	// called during group by result set scan
	auto target = targets_outer(targets);
	assert(target->r != -1);
	assert(rtype(self, target->r) == TYPE_STORE);

	// read aggregate value based on the function and type
	int  agg_op;
	int  agg_type;
	auto agg = ast_agg_of(ast);
	if (agg->id == AGG_LAMBDA)
	{
		assert(agg->expr_seed_type != -1);
		return op3(self, CSTORE_READ, rpin(self, agg->expr_seed_type),
		           target->id, agg->order);
	}
	switch (agg->id) {
	case AGG_INT_COUNT:
	case AGG_INT_COUNT_DISTINCT:
		agg_op   = CCOUNT;
		agg_type = TYPE_INT;
		break;
	case AGG_INT_MIN:
	case AGG_INT_MAX:
	case AGG_INT_SUM:
		agg_op   = CAGG;
		agg_type = TYPE_INT;
		break;
	case AGG_DOUBLE_MIN:
	case AGG_DOUBLE_MAX:
	case AGG_DOUBLE_SUM:
		agg_op   = CAGG;
		agg_type = TYPE_DOUBLE;
		break;
	case AGG_INT_AVG:
		agg_op   = CAVGI;
		agg_type = TYPE_INT;
		break;
	case AGG_DOUBLE_AVG:
		agg_op   = CAVGF;
		agg_type = TYPE_DOUBLE;
		break;
	default:
		abort();
		break;
	}
	return op3(self, agg_op, rpin(self, agg_type), target->id,
	           agg->order);
}

hot static inline int
emit_aggregate_key(Compiler* self, Targets* targets, Ast* ast)
{
	// SELECT GROUP BY 1
	// SELECT GROUP BY alias

	// called during group by result set scan
	auto target = targets_outer(targets);
	assert(target->r != -1);
	assert(rtype(self, target->r) == TYPE_STORE);

	// group by expr
	auto column = ast_group_of(ast)->column;
	assert(column->type != -1);

	// read aggregate key value
	return op3(self, CSTORE_READ, rpin(self, column->type), target->id,
	           column->order);
}

hot static inline int
emit_self(Compiler* self, Ast* ast)
{
	auto agg = ast_agg_of(ast->r);
	auto select = ast_select_of(agg->select);
	assert(agg->rseed != -1);

	// read aggregate state, return seed value if the
	// state is null

	// push agg order
	auto r = op2(self, CINT, rpin(self, TYPE_INT), agg->order);
	op1(self, CPUSH, r);
	runpin(self, r);

	// CSELF
	return op4(self, CSELF, rpin(self, agg->expr_seed_type),
	           select->rset_agg,
	           select->rset_agg_row, agg->rseed);
}

hot static inline int
emit_operator(Compiler* self, Targets* targets, Ast* ast, int op)
{
	int l = emit_expr(self, targets, ast->l);
	int r = emit_expr(self, targets, ast->r);
	return cast_operator(self, ast->r, op, l, r);
}

hot static inline bool
emit_func_typederive(Compiler* self, int r, int* type)
{
	auto this = rtype(self, r);
	if (this == TYPE_NULL)
		return true;
	if (*type == TYPE_NULL)
		*type = this;
	return *type == this;
}

hot int
emit_func(Compiler* self, Targets* targets, Ast* ast)
{
	// (function_name, args)
	auto func = ast_func_of(ast);
	auto args = ast->r;

	// push arguments
	auto fn = func->fn;
	auto fn_type = fn->type;
	auto current = args->l;
	while (current)
	{
		int r = emit_expr(self, targets, current);
		// ensure that the function has identical types, if type is derived
		if (fn->flags & FN_DERIVE)
			if (! emit_func_typederive(self, r, &fn_type))
				stmt_error(self->current, current, "argument type must match other arguments");
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// register function call, if it has context
	int call_id = -1;
	if (fn->flags & FN_CONTEXT)
		call_id = code_data_add_call(self->code_data, fn);

	// CALL
	return op4(self, CCALL, rpin(self, fn_type), (intptr_t)fn,
	           args->integer, call_id);
}

hot static inline int
emit_method(Compiler* self, Targets* targets, Ast* ast)
{
	// expr, method(path, [args])
	auto expr = ast->l;
	auto func = ast_func_of(ast->r);
	auto args = func->ast.r;

	auto fn = func->fn;
	auto fn_type = fn->type;

	// use expression as the first argument to the call
	int r = emit_expr(self, targets, expr);
	if (fn->flags & FN_DERIVE)
		emit_func_typederive(self, r, &fn_type);
	op1(self, CPUSH, r);
	runpin(self, r);

	// push the rest of the arguments
	int argc = 1;
	if (args)
	{
		auto current = args->l;
		while (current)
		{
			r = emit_expr(self, targets, current);
			// ensure that the function has identical types, if type is derived
			if (fn->flags & FN_DERIVE)
				if (! emit_func_typederive(self, r, &fn_type))
					stmt_error(self->current, current, "argument type must match other arguments");
			op1(self, CPUSH, r);
			runpin(self, r);
			current = current->next;
		}
		argc += args->integer;
	}

	// register function call, if it has context
	int call_id = -1;
	if (fn->flags & FN_CONTEXT)
		call_id = code_data_add_call(self->code_data, fn);

	// CALL
	return op4(self, CCALL, rpin(self, fn_type), (intptr_t)fn, argc, call_id);
}

hot static inline int
emit_between(Compiler* self, Targets* targets, Ast* ast)
{
	//    . BETWEEN .
	// expr       . AND .
	//            x     y
	auto expr = ast->l;
	auto x = ast->r->l;
	auto y = ast->r->r;
	// expr >= x AND expr <= y
	Ast gte;
	gte.id = KGTE;
	gte.l  = expr;
	gte.r  = x;
	Ast lte;
	lte.id = KLTE;
	lte.l  = expr;
	lte.r  = y;
	Ast and;
	and.id = KAND;
	and.l  = &gte;
	and.r  = &lte;
	Ast not;
	not.id = KNOT;
	not.l  = &and;
	not.r  = NULL;
	auto op = !ast->integer ? &not : &and;
	return emit_expr(self, targets, op);
}

hot static inline int
emit_case(Compiler* self, Targets* targets, Ast* ast)
{
	auto cs = ast_case_of(ast);
	// CASE [expr] [WHEN expr THEN expr]
	//             [ELSE expr]
	// END
	int rresult = rpin(self, TYPE_NULL);

	// jmp to start
	int _start_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _stop_jmp
	int _stop_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _stop */);

	// foreach when
		// if cs->expr
			// expr
			// WHEN expr
			// equ
			// jntr _next
			// set THEN result
			// jmp _stop

		// else
			// WHEN expr
			// jntr _next
			// set THEN result
			// jmp _stop

	// _else
		// ELSE result
	// _stop

	// _start
	int _start = op_pos(self);
	op_set_jmp(self, _start_jmp, _start);

	auto node = cs->when.list;
	for (; node; node = node->next)
	{
		auto when = node->ast;

		int rcond;
		if (cs->expr)
		{
			// WHEN expr = case_expr THEN result
			auto rexpr = emit_expr(self, targets, cs->expr);
			auto rwhen = emit_expr(self, targets, when->l);
			rcond = cast_operator(self, when->l, OP_EQU, rexpr, rwhen);
		} else
		{
			// WHEN expr THEN result
			rcond = emit_expr(self, targets, when->l);
		}

		// jntr _next
		int _next_jntr = op_pos(self);
		op2(self, CJNTR, 0 /* _next */, rcond);

		// THEN expr
		int rthen = emit_expr(self, targets, when->r);

		// set result type to the type of first result
		if (rtype(self, rresult) == TYPE_NULL)
			rmap_assign(&self->map, rresult, rtype(self, rthen));
		else
		if (rtype(self, rresult) != rtype(self, rthen) &&
		    rtype(self, rthen) != TYPE_NULL)
			stmt_error(self->current, when->r, "CASE expression type mismatch");

		op2(self, CMOV, rresult, rthen);
		runpin(self, rthen);

		op1(self, CJMP, _stop_jmp);

		// _next
		op_at(self, _next_jntr)->a = op_pos(self);
		runpin(self, rcond);
	}

	// _else
	int relse;
	if (cs->expr_else)
	{
		relse = emit_expr(self, targets, cs->expr_else);

		// set result type to the type of first result
		if (rtype(self, rresult) == TYPE_NULL)
			rmap_assign(&self->map, rresult, rtype(self, relse));
		else
		if (rtype(self, rresult) != rtype(self, relse) &&
		    rtype(self, relse) != TYPE_NULL)
			stmt_error(self->current, cs->expr_else, "CASE expression type mismatch");
	} else
	{
		relse = op1(self, CNULL, rpin(self, TYPE_NULL));
	}
	op2(self, CMOV, rresult, relse);
	runpin(self, relse);

	// _stop
	int _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);
	op0(self, CNOP);
	return rresult;
}

hot static inline int
emit_is(Compiler* self, Targets* targets, Ast* ast)
{
	if (!ast->r || ast->r->id != KNULL)
		stmt_error(self->current, ast->r, "NOT or NULL expected");
	auto rexpr = emit_expr(self, targets, ast->l);
	auto rresult = op2(self, CIS, rpin(self, TYPE_BOOL), rexpr);
	runpin(self, rexpr);

	// [not]
	if (! ast->integer)
	{
		int rc;
		rc = op2(self, CNOT, rpin(self, TYPE_BOOL), rresult);
		runpin(self, rresult);
		rresult = rc;
	}
	return rresult;
}

hot static inline int
emit_in(Compiler* self, Targets* targets, Ast* ast)
{
	auto expr = ast->l;
	auto args = ast->r;
	assert(args->id == KARGS);

	// expr [NOT] IN (value, ...)
	int rexpr = emit_expr(self, targets, expr);

	// push values
	auto current = args->l;
	while (current)
	{
		int r;
		if (current->id == KSELECT)
		{
			auto select = ast_select_of(current);
			if (select->ret.columns.count > 1)
				stmt_error(self->current, &select->ast, "subquery must return one column");
			r = emit_select(self, current);
		} else {
			r = emit_expr(self, targets, current);
		}
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// IN
	int rin = op3(self, CIN, rpin(self, TYPE_BOOL), rexpr, args->integer);
	runpin(self, rexpr);

	// [not]
	if (! ast->integer)
	{
		auto rc = op2(self, CNOT, rpin(self, TYPE_BOOL), rin);
		runpin(self, rin);
		rin = rc;
	}
	return rin;
}

hot static inline int
emit_match(Compiler* self, Targets* targets, Ast* ast)
{
	//  . ANY|ALL .
	//  a         op .
	//               b
	int a = emit_expr(self, targets, ast->l);
	int b;
	if (ast->r->r->id == KSELECT)
	{
		auto select = ast_select_of(ast->r->r);
		if (select->ret.columns.count > 1)
			stmt_error(self->current, &select->ast, "subquery must return one column");
		b = emit_select(self, ast->r->r);
	} else {
		b = emit_expr(self, targets, ast->r->r);
	}
	int op;
	switch (ast->r->id) {
	case '=':   op = MATCH_EQU;
		break;
	case KNEQU: op = MATCH_NEQU;
		break;
	case '>':   op = MATCH_GT;
		break;
	case KGTE:  op = MATCH_GTE;
		break;
	case '<':   op = MATCH_LT;
		break;
	case KLTE:  op = MATCH_LTE;
		break;
	default:
		abort();
		break;
	}
	int rc;
	switch (ast->id) {
	case KANY:
		rc = op4(self, CANY, rpin(self, TYPE_BOOL), a, b, op);
		break;
	case KALL:
		rc = op4(self, CALL, rpin(self, TYPE_BOOL), a, b, op);
		break;
	default:
		abort();
		break;
	}
	runpin(self, a);
	runpin(self, b);
	return rc;
}

hot static inline int
emit_at_timezone(Compiler* self, Targets* targets, Ast* ast)
{
	// public.at_timezone(time, timezone)
	Str schema;
	str_set(&schema, "public", 6);
	Str name;
	str_set(&name, "at_timezone", 11);

	// find and call function
	auto fn = function_mgr_find(self->parser.function_mgr, &schema, &name);
	assert(fn);

	// push arguments
	int r = emit_expr(self, targets, ast->l);
	op1(self, CPUSH, r);
	runpin(self, r);

	r = emit_expr(self, targets, ast->r);
	op1(self, CPUSH, r);
	runpin(self, r);

	// CALL
	return op4(self, CCALL, rpin(self, fn->type), (intptr_t)fn, 2, -1);
}

hot int
emit_expr(Compiler* self, Targets* targets, Ast* ast)
{
	switch (ast->id) {
	// consts
	case KNULL:
		return op1(self, CNULL, rpin(self, TYPE_NULL));
	case KTRUE:
		return op2(self, CBOOL, rpin(self, TYPE_BOOL), 1);
	case KFALSE:
		return op2(self, CBOOL, rpin(self, TYPE_BOOL), 0);
	case KINT:
		return op2(self, CINT, rpin(self, TYPE_INT), ast->integer);
	case KREAL:
		return op2(self, CDOUBLE, rpin(self, TYPE_DOUBLE),
		           code_data_add_double(self->code_data, ast->real));
	case KSTRING:
		return emit_string(self, &ast->string, ast->string_escape);

	// timestamp/interval
	case KTIMESTAMP:
	{
		Timestamp ts;
		timestamp_init(&ts);
		if (unlikely(error_catch( timestamp_set(&ts, &ast->string) )))
			stmt_error(self->current, ast, "invalid timestamp value");
		return op2(self, CTIMESTAMP, rpin(self, TYPE_TIMESTAMP),
		           timestamp_get_unixtime(&ts, self->parser.local->timezone));
	}
	case KINTERVAL:
	{
		int offset = code_data_offset(self->code_data);
		auto iv = (Interval*)buf_claim(&self->code_data->data, sizeof(Interval));
		interval_init(iv);
		if (unlikely(error_catch( interval_set(iv, &ast->string) )))
			stmt_error(self->current, ast, "invalid interval value");
		return op2(self, CINTERVAL, rpin(self, TYPE_INTERVAL), offset);
	}
	case KDATE:
	{
		int julian;
		if (unlikely(error_catch( julian = date_set(&ast->string) )))
			stmt_error(self->current, ast, "invalid date value");
		return op2(self, CDATE, rpin(self, TYPE_DATE), julian);
	}
	case KCURRENT_TIMESTAMP:
		return op2(self, CTIMESTAMP, rpin(self, TYPE_TIMESTAMP),
		           self->parser.local->time_us);
	case KCURRENT_DATE:
		return op2(self, CDATE, rpin(self, TYPE_DATE),
		           timestamp_date(self->parser.local->time_us));

	// vector
	case KVECTOR:
		return op2(self, CVECTOR, rpin(self, TYPE_VECTOR),
		           ast->integer);

	// uuid
	case KUUID:
	{
		int offset = code_data_offset(self->code_data);
		auto uuid = (Uuid*)buf_claim(&self->code_data->data, sizeof(Uuid));
		uuid_init(uuid);
		if (uuid_set_nothrow(uuid, &ast->string) == -1)
			stmt_error(self->current, ast, "invalid uuid value");
		return op2(self, CUUID, rpin(self, TYPE_UUID), offset);
	}

	// json
	case '{':
	case KARRAY:
		return emit_json(self, targets, ast);

	// column
	case KNAME:
		return emit_name(self, targets, ast);
	case KNAME_COMPOUND:
		return emit_name_compound(self, targets, ast);

	// aggregate
	case KAGGR:
		return emit_aggregate(self, targets, ast);
	case KAGGRKEY:
		return emit_aggregate_key(self, targets, ast);
	case KARROW:
		// lambda (during result set scan)
		return emit_aggregate(self, targets, ast->r);
	case KSELF:
		// lambda state read (during table scan)
		return emit_self(self, ast);

	// operators
	case KNEQU:
	{
		auto r = emit_operator(self, targets, ast, OP_EQU);
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}
	case '=':
		return emit_operator(self, targets, ast, OP_EQU);
	case KGTE:
		return emit_operator(self, targets, ast, OP_GTE);
	case '>':
		return emit_operator(self, targets, ast, OP_GT);
	case KLTE:
		return emit_operator(self, targets, ast, OP_LTE);
	case '<':
		return emit_operator(self, targets, ast, OP_LT);
	case '+':
		return emit_operator(self, targets, ast, OP_ADD);
	case '-':
		return emit_operator(self, targets, ast, OP_SUB);
	case '*':
		return emit_operator(self, targets, ast, OP_MUL);
	case '/':
		return emit_operator(self, targets, ast, OP_DIV);
	case '%':
		return emit_operator(self, targets, ast, OP_MOD);
	case KCAT:
		return emit_operator(self, targets, ast, OP_CAT);
	case '[':
		return emit_operator(self, targets, ast, OP_IDX);
	case '.':
		return emit_operator(self, targets, ast, OP_DOT);
	case KLIKE:
	{
		auto r = emit_operator(self, targets, ast, OP_LIKE);
		if (ast->integer)
			return r;
		// [not]
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}
	case '|':
		return emit_operator(self, targets, ast, OP_BOR);
	case '^':
		return emit_operator(self, targets, ast, OP_BXOR);
	case '&':
		return emit_operator(self, targets, ast, OP_BAND);
	case KSHL:
		return emit_operator(self, targets, ast, OP_BSHL);
	case KSHR:
		return emit_operator(self, targets, ast, OP_BSHR);

	// logic
	case KAND:
	case KOR:
	{
		int l = emit_expr(self, targets, ast->l);
		int r = emit_expr(self, targets, ast->r);
		int rc;
		rc = op3(self, ast->id == KAND? CAND: COR,
		         rpin(self, TYPE_BOOL), l, r);
		runpin(self, l);
		runpin(self, r);
		return rc;
	}
	case KNOT:
	{
		int r = emit_expr(self, targets, ast->l);
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}

	// unary operations
	case KNEG:
	{
		int r = emit_expr(self, targets, ast->l);
		int rt = rtype(self, r);
		int rneg = -1;
		if (rt == TYPE_INT)
			rneg = op2(self, CNEGI, rpin(self, TYPE_INT), r);
		else
		if (rt == TYPE_DOUBLE)
			rneg = op2(self, CNEGF, rpin(self, TYPE_DOUBLE), r);
		else
		if (rt == TYPE_INTERVAL)
			rneg = op2(self, CNEGL, rpin(self, TYPE_INTERVAL), r);
		else
			stmt_error(self->current, ast->l, "unsupported operation type %s", type_of(rt));
		runpin(self, r);
		return rneg;
	}
	case '~':
	{
		int r = emit_expr(self, targets, ast->l);
		int rt = rtype(self, r);
		if (rt != TYPE_INT)
			stmt_error(self->current, ast->l, "unsupported operation type %s", type_of(rt));
		auto rinv = op2(self, CBINVI, rpin(self, TYPE_INT), r);
		runpin(self, r);
		return rinv;
	}

	// function/method call
	case KFUNC:
		return emit_func(self, targets, ast);
	case KMETHOD:
		return emit_method(self, targets, ast);

	// subquery
	case KSELECT:
	{
		// ensure that subquery returns one column
		auto select = ast_select_of(ast);
		auto columns = &select->ret.columns;
		if (columns->count > 1)
			stmt_error(self->current, ast, "subquery must return only one column");
		auto r = emit_select(self, ast);
		// return first column of the first row and free the set
		auto rresult = op2(self, CSET_RESULT, rpin(self, columns_first(columns)->type), r);
		runpin(self, r);
		return rresult;
	}

	// case
	case KCASE:
		return emit_case(self, targets, ast);

	// between
	case KBETWEEN:
		return emit_between(self, targets, ast);

	// is
	case KIS:
		return emit_is(self, targets, ast);

	// in
	case KIN:
		return emit_in(self, targets, ast);

	// any|all
	case KANY:
	case KALL:
		return emit_match(self, targets, ast);

	// exists
	case KEXISTS:
	{
		auto r = emit_select(self, ast->r);
		auto rc = op2(self, CEXISTS, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rc;
	}

	// at timezone
	case KAT:
		return emit_at_timezone(self, targets, ast);

	default:
		assert(0);
		break;
	}

	assert(0);
	return -1;
}
