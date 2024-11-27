
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

hot int
emit_string(Compiler* self, Str* string, bool escape)
{
	int offset;
	if (escape)
		offset = code_data_add_string_unescape(&self->code_data, string);
	else
		offset = code_data_add_string(&self->code_data, string);
	return op2(self, CSTRING, rpin(self, TYPE_STRING), offset);
}

hot static inline int
emit_json(Compiler* self, Target* target, Ast* ast, int op)
{
	auto args = ast->l;
	assert(args->id == KARGS);

	// push arguments
	auto current = args->l;
	while (current)
	{
		int r = emit_expr(self, target, current);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// op
	return op2(self, op, rpin(self, TYPE_JSON), args->integer);
}

hot static inline int
emit_column(Compiler* self, Target* target, Str* name, bool excluded)
{
	// find unique column name in the target
	bool conflict = false;
	auto column = columns_find_noconflict(target->from_columns, name, &conflict);
	if (! column)
	{
		if (conflict)
			error("<%.*s.%.*s> column name is ambiguous",
			      str_size(&target->name), str_of(&target->name),
			      str_size(name), str_of(name));
		else
			error("<%.*s.%.*s> column not found",
			      str_size(&target->name), str_of(&target->name),
			      str_size(name), str_of(name));
	}

	// read excluded column
	if (unlikely(excluded))
		return op3(self, CEXCLUDED, rpin(self, column->type),
		           target->id, column->order);

	// generate cursor read based on the target
	int r;
	if (target->type == TARGET_TABLE ||
	    target->type == TARGET_TABLE_SHARED)
	{
		int op;
		switch (column->type) {
		case TYPE_BOOL:
			op = CCURSOR_READB;
			break;
		case TYPE_INT:
		{
			switch (column->type_size) {
			case 1: op = CCURSOR_READI8;
				break;
			case 2: op = CCURSOR_READI16;
				break;
			case 4: op = CCURSOR_READI32;
				break;
			case 8: op = CCURSOR_READI64;
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
			case 4: op = CCURSOR_READF;
				break;
			case 8: op = CCURSOR_READD;
				break;
			default:
				abort();
				break;
			}
			break;
		}
		case TYPE_STRING:
			op = CCURSOR_READS;
			break;
		case TYPE_JSON:
			op = CCURSOR_READJ;
			break;
		case TYPE_TIMESTAMP:
			op = CCURSOR_READT;
			break;
		case TYPE_INTERVAL:
			op = CCURSOR_READL;
			break;
		case TYPE_VECTOR:
			op = CCURSOR_READV;
			break;
		default:
			abort();
			break;
		}
		r = op3(self, op, rpin(self, column->type),
		        target->id, column->order);
	} else
	{
		assert(target->r != -1);
		switch (rtype(self, target->r)) {
		case TYPE_SET:
			r = op3(self, CCURSOR_SET_READ, rpin(self, column->type),
			        target->id, column->order);
			break;
		case TYPE_MERGE:
			r = op3(self, CCURSOR_MERGE_READ, rpin(self, column->type),
			        target->id, column->order);
			break;
		default:
			abort();
		}
	}
	return r;
}

hot static inline int
emit_name(Compiler* self, Target* target, Ast* ast)
{
	// SELECT name
	auto name = &ast->string;

	auto target_list = compiler_target_list(self);
	if (target_list->count == 0)
		error("<%.*s> column not found",
		      str_size(name), str_of(name));

	if (unlikely(target == NULL))
		error("<%.*s> target column cannot be found",
		      str_size(name), str_of(name));

	// SELECT name FROM
	if (target_is_join(target))
		error("<%.*s> column requires explicit target name",
		      str_size(name), str_of(name));

	// todo: lambda?
	return emit_column(self, target, name, false);
}

hot static inline int
emit_name_compound(Compiler* self, Target* target, Ast* ast)
{
	// target.column[.path]
	// column.path
	Str name;
	str_split(&ast->string, &name, '.');

	Str path;
	str_init(&path);
	str_set_str(&path, &ast->string);

	// check if the first path is a target name
	auto target_list = compiler_target_list(self);
	if (target_list->count == 0)
		error("<%.*s> column not found",
		      str_size(&name), str_of(&name));

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
	} else
	{
		// find target
		auto match = target_list_match(target_list, &name);
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

		if (! target)
			error("<%.*s> target not found", str_size(&name),
			      str_of(&name));
	}

	// column[.path]
	auto rcolumn = emit_column(self, target, &name, excluded);

	if (str_empty(&path))
		return rcolumn;

	// ensure column is a json
	if (rtype(self, rcolumn) != TYPE_JSON)
		error("'.': column <%.*s.%.*s> is not a json",
		      str_size(&target->name), str_of(&target->name),
		      str_size(&name), str_of(&name));

	// column.path
	int rstring = emit_string(self, &path, false);
	return cast_operator(self, OP_DOT, rcolumn, rstring);
}

hot static inline int
emit_aggregate(Compiler* self, Target* target, Ast* ast)
{
	// SELECT aggr GROUP BY
	// SELECT GROUP BY HAVING aggr
	assert(target->r != -1);
	assert(rtype(self, target->r) == TYPE_SET);

	// read aggregate value based on the function and type
	int  agg_op;
	int  agg_type;
	auto agg = ast_agg_of(ast);
	switch (agg->id) {
	case AGG_INT_COUNT:
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
		agg_op   = CAVGD;
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
emit_operator(Compiler* self, Target* target, Ast* ast, int op)
{
	int l = emit_expr(self, target, ast->l);
	int r = emit_expr(self, target, ast->r);
	return cast_operator(self, op, l, r);
}

hot static inline void
emit_call_typederive(Compiler* self, int r, int* type, bool* type_match)
{
	auto this = rtype(self, r);
	if (this == TYPE_NULL)
		return;
	if (*type == TYPE_NULL) {
		*type = this;
	} else
	if (*type != this) {
		*type = this;
		*type_match = false;
	}
}

hot static inline int
emit_call(Compiler* self, Target* target, Ast* ast)
{
	// (function_name, args)
	auto call = ast_call_of(ast);
	auto args = ast->r;

	auto fn = call->fn;
	auto fn_type = fn->ret;
	bool fn_type_match = true;

	// push arguments
	auto current = args->l;
	while (current)
	{
		int r = emit_expr(self, target, current);
		if (fn->flags & FN_TYPE_DERIVE)
			emit_call_typederive(self, r, &fn_type, &fn_type_match);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// ensure that the function has identical types, if type is derived
	if (fn->flags & FN_TYPE_DERIVE && !fn_type_match)
		error("%.*s.%.*s(): arguments types mismatch",
		      str_size(&fn->schema),
		      str_of(&fn->schema),
		      str_size(&fn->name),
		      str_of(&fn->name));

	// register function call, if it has context
	int call_id = -1;
	if (fn->flags & FN_CONTEXT)
		call_id = code_data_add_call(&self->code_data, fn);

	// CALL
	return op4(self, CCALL, rpin(self, fn_type), (intptr_t)fn, args->integer, call_id);
}

hot static inline int
emit_call_method(Compiler* self, Target* target, Ast* ast)
{
	// expr, method(path, [args])
	auto expr = ast->l;
	auto call = ast_call_of(ast->r);
	auto args = call->ast.r;

	auto fn = call->fn;
	auto fn_type = fn->ret;
	bool fn_type_match = true;

	// use expression as the first argument to the call
	int r = emit_expr(self, target, expr);
	if (fn->flags & FN_TYPE_DERIVE)
		emit_call_typederive(self, r, &fn_type, &fn_type_match);
	op1(self, CPUSH, r);
	runpin(self, r);

	// push the rest of the arguments
	int argc = 1;
	if (args)
	{
		auto current = args->l;
		while (current)
		{
			r = emit_expr(self, target, current);
			if (fn->flags & FN_TYPE_DERIVE)
				emit_call_typederive(self, r, &fn_type, &fn_type_match);
			op1(self, CPUSH, r);
			runpin(self, r);
			current = current->next;
		}
		argc += args->integer;
	}

	// ensure that the function has identical types, if type is derived
	if (fn->flags & FN_TYPE_DERIVE && !fn_type_match)
		error("%.*s.%.*s(): arguments types mismatch",
		      str_size(&fn->schema),
		      str_of(&fn->schema),
		      str_size(&fn->name),
		      str_of(&fn->name));

	// register function call, if it has context
	int call_id = -1;
	if (fn->flags & FN_CONTEXT)
		call_id = code_data_add_call(&self->code_data, fn);

	// CALL
	return op4(self, CCALL, rpin(self, fn_type), (intptr_t)fn, argc, call_id);
}

hot static inline int
emit_between(Compiler* self, Target* target, Ast* ast)
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
	return emit_expr(self, target, op);
}

hot static inline int
emit_case(Compiler* self, Target* target, Ast* ast)
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
			auto rexpr = emit_expr(self, target, cs->expr);
			auto rwhen = emit_expr(self, target, when->l);
			rcond = cast_operator(self, OP_EQU, rexpr, rwhen);
		} else
		{
			// WHEN expr THEN result
			rcond = emit_expr(self, target, when->l);
		}

		// jntr _next
		int _next_jntr = op_pos(self);
		op2(self, CJNTR, 0 /* _next */, rcond);

		// THEN expr
		int rthen = emit_expr(self, target, when->r);

		// set result type to the type of first result
		if (rtype(self, rresult) == TYPE_NULL)
			self->map.map[rresult] = rtype(self, rthen);
		else
		if (rtype(self, rresult) != rtype(self, rthen) &&
		    rtype(self, rthen) != TYPE_NULL)
			error("CASE expr types must match");

		op2(self, CSWAP, rresult, rthen);
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
		relse = emit_expr(self, target, cs->expr_else);

		// set result type to the type of first result
		if (rtype(self, rresult) == TYPE_NULL)
			self->map.map[rresult] = rtype(self, relse);
		else
		if (rtype(self, rresult) != rtype(self, relse) &&
		    rtype(self, relse) != TYPE_NULL)
			error("CASE expr types must match");
	} else
	{
		relse = op1(self, CNULL, rpin(self, TYPE_NULL));
	}
	op2(self, CSWAP, rresult, relse);
	runpin(self, relse);

	// _stop
	int _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);
	op0(self, CNOP);
	return rresult;
}

hot static inline int
emit_is(Compiler* self, Target* target, Ast* ast)
{
	if (!ast->r || ast->r->id != KNULL)
		error("IS [NOT] <NULL> expected");
	auto rexpr = emit_expr(self, target, ast->l);
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
emit_in(Compiler* self, Target* target, Ast* ast)
{
	auto expr = ast->l;
	auto args = ast->r;
	assert(args->id == KARGS);

	// expr
	int rresult = op2(self, CBOOL, rpin(self, TYPE_BOOL), false);

	// jmp to start
	int _start_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _start */);

	// _stop_jmp
	int _stop_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _stop */);

	// foreach arg

		// expr
		// in
		// jntr _next
		// set result to true
		// jmp _stop

	// _stop

	// _start
	int _start = op_pos(self);
	op_set_jmp(self, _start_jmp, _start);

	auto current = args->l;
	while (current)
	{
		int rexpr = emit_expr(self, target, expr);
		int r = emit_expr(self, target, current);
		int rin = op3(self, CIN, rpin(self, TYPE_BOOL), rexpr, r);
		runpin(self, rexpr);
		runpin(self, r);

		// jntr _next
		int _next_jntr = op_pos(self);
		op2(self, CJNTR, 0 /* _next */, rin);
		op2(self, CBOOL, rresult, true);
		op1(self, CJMP, _stop_jmp);

		// _next
		op_at(self, _next_jntr)->a = op_pos(self);

		runpin(self, rin);
		current = current->next;
	}

	// _stop
	int _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);

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
emit_match(Compiler* self, Target* target, Ast* ast)
{
	//  . ANY|ALL .
	//  a         op .
	//               b
	int a = emit_expr(self, target, ast->l);
	int b = emit_expr(self, target, ast->r->r);
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
emit_at_timezone(Compiler* self, Target* target, Ast* ast)
{
	// public.at_timezone(time, timezone)
	Str schema;
	str_set(&schema, "public", 6);
	Str name;
	str_set(&name, "at_timezone", 11);

	// find and call function
	auto fn = function_mgr_find(self->parser.function_mgr, &schema, &name);
	if (! fn)
		error("at_timezone(): function not found");

	// push arguments
	int r = emit_expr(self, target, ast->l);
	op1(self, CPUSH, r);
	runpin(self, r);

	r = emit_expr(self, target, ast->r);
	op1(self, CPUSH, r);
	runpin(self, r);

	// CALL
	return op4(self, CCALL, rpin(self, fn->ret), (intptr_t)fn, 2, -1);
}

hot int
emit_expr(Compiler* self, Target* target, Ast* ast)
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
		           code_data_add_double(&self->code_data, ast->real));
	case KSTRING:
		return emit_string(self, &ast->string, ast->string_escape);
	case KCURRENT_TIMESTAMP:
		return op2(self, CTIMESTAMP, rpin(self, TYPE_TIMESTAMP),
		           self->parser.local->time_us);
	case KTIMESTAMP:
		return op2(self, CTIMESTAMP, rpin(self, TYPE_TIMESTAMP), ast->integer);
	case KINTERVAL:
	{
		int offset = code_data_offset(&self->code_data);
		buf_write(&self->code_data.data, &ast->interval, sizeof(Interval));
		return op2(self, CINTERVAL, rpin(self, TYPE_INTERVAL), offset);
	}

	// json
	case '{':
		return emit_json(self, target, ast, CJSON_OBJ);
	case KARRAY:
		return emit_json(self, target, ast, CJSON_ARRAY);

#if 0
	// argument
	case KARGID:
		return op2(self, CARG, rpin(self, TYPE_JSON), ast->integer);
	case KCTEID:
		return op2(self, CCTE_GET, rpin(self), ast->integer);
#endif

	// column
	case KNAME:
		return emit_name(self, target, ast);
	case KNAME_COMPOUND:
		return emit_name_compound(self, target, ast);

	// aggregate
	case KAGGR:
		return emit_aggregate(self, target, ast);

	// operators
	case KNEQU:
	{
		auto r = emit_operator(self, target, ast, OP_EQU);
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}
	case '=':
		return emit_operator(self, target, ast, OP_EQU);
	case KGTE:
		return emit_operator(self, target, ast, OP_GTE);
	case '>':
		return emit_operator(self, target, ast, OP_GT);
	case KLTE:
		return emit_operator(self, target, ast, OP_LTE);
	case '<':
		return emit_operator(self, target, ast, OP_LT);
	case '+':
		return emit_operator(self, target, ast, OP_ADD);
	case '-':
		return emit_operator(self, target, ast, OP_SUB);
	case '*':
		return emit_operator(self, target, ast, OP_MUL);
	case '/':
		return emit_operator(self, target, ast, OP_DIV);
	case '%':
		return emit_operator(self, target, ast, OP_MOD);
	case KCAT:
		return emit_operator(self, target, ast, OP_CAT);
	case '[':
		return emit_operator(self, target, ast, OP_IDX);
	case '.':
		return emit_operator(self, target, ast, OP_DOT);
	case KLIKE:
	{
		auto r = emit_operator(self, target, ast, OP_LIKE);
		if (ast->integer)
			return r;
		// [not]
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}
	case '|':
		return emit_operator(self, target, ast, OP_BOR);
	case '^':
		return emit_operator(self, target, ast, OP_BXOR);
	case '&':
		return emit_operator(self, target, ast, OP_BAND);
	case KSHL:
		return emit_operator(self, target, ast, OP_BSHL);
	case KSHR:
		return emit_operator(self, target, ast, OP_BSHR);

	// logic
	case KAND:
	case KOR:
	{
		int l = emit_expr(self, target, ast->l);
		int r = emit_expr(self, target, ast->r);
		int rc;
		rc = op3(self, ast->id == KAND? CAND: COR,
		         rpin(self, TYPE_BOOL), l, r);
		runpin(self, l);
		runpin(self, r);
		return rc;
	}
	case KNOT:
	{
		int r = emit_expr(self, target, ast->l);
		auto rnot = op2(self, CNOT, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rnot;
	}

	// unary operations
	case KNEG:
	{
		int r = emit_expr(self, target, ast->l);
		int rt = rtype(self, r);
		int rneg;
		if (rt == TYPE_INT)
			rneg = op2(self, CNEGI, rpin(self, TYPE_INT), r);
		else
		if (rt == TYPE_DOUBLE)
			rneg = op2(self, CNEGD, rpin(self, TYPE_DOUBLE), r);
		else
			error("'-': unsupported operation type");
		runpin(self, r);
		return rneg;
	}
	case '~':
	{
		int r = emit_expr(self, target, ast->l);
		if (rtype(self, r) != TYPE_INT)
			error("'-': unsupported operation type");
		auto rinv = op2(self, CBINVI, rpin(self, TYPE_INT), r);
		runpin(self, r);
		return rinv;
	}

	// function/method call
	case KCALL:
		return emit_call(self, target, ast);
	case KMETHOD:
		return emit_call_method(self, target, ast);

	// sub-query
	case KSELECT:
		return emit_select(self, ast);

	// case
	case KCASE:
		return emit_case(self, target, ast);

	// between
	case KBETWEEN:
		return emit_between(self, target, ast);

	// is
	case KIS:
		return emit_is(self, target, ast);

	// in
	case KIN:
		return emit_in(self, target, ast);

	// any|all
	case KANY:
	case KALL:
		return emit_match(self, target, ast);

	// exists
	case KEXISTS:
	{
		auto r = emit_expr(self, target, ast->r);
		auto rc = op2(self, CEXISTS, rpin(self, TYPE_BOOL), r);
		runpin(self, r);
		return rc;
	}

	// at timezone
	case KAT:
		return emit_at_timezone(self, target, ast);

	default:
		assert(0);
		break;
	}

	assert(0);
	return -1;
}
