
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
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
		offset = code_data_add_string(&self->code_data, string);
	else
		offset = code_data_add_string_unescape(&self->code_data, string);
	return op2(self, CSTRING, rpin(self), offset);
}

hot static inline int
emit_operator(Compiler* self, Target* target, Ast* ast, int op)
{
	int l = emit_expr(self, target, ast->l);
	int r = emit_expr(self, target, ast->r);
	int rc;
	rc = op3(self, op, rpin(self), l, r);
	runpin(self, l);
	runpin(self, r);
	return rc;
}

hot static inline int
emit_unary(Compiler* self, Target* target, Ast* ast, int op)
{
	int l = emit_expr(self, target, ast->l);
	int rc;
	rc = op2(self, op, rpin(self), l);
	runpin(self, l);
	return rc;
}

hot static inline int
emit_obj(Compiler* self, Target* target, Ast* ast, int op)
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
	return op2(self, op, rpin(self), args->integer);
}

hot static inline int
emit_call(Compiler* self, Target* target, Ast* ast)
{
	// (function_name, args)
	auto call = ast_call_of(ast);
	auto args = ast->r;

	// replace now() with local transaction time
	if (str_compare_raw(&call->fn->name, "now", 3) &&
		str_compare_raw(&call->fn->schema, "public", 6))
	{
		auto time = self->parser.local->time_us;
		return op2(self, CTIMESTAMP, rpin(self), time);
	}

	// push arguments
	auto current = args->l;
	while (current)
	{
		int r = emit_expr(self, target, current);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// CALL
	return op3(self, CCALL, rpin(self), (intptr_t)call->fn, args->integer);
}

hot static inline int
emit_call_method(Compiler* self, Target* target, Ast* ast)
{
	// expr, method(path, [args])
	auto expr = ast->l;
	auto call = ast_call_of(ast->r);
	auto args = call->ast.r;

	// use expression as the first argument to the call
	int r = emit_expr(self, target, expr);
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
			op1(self, CPUSH, r);
			runpin(self, r);
			current = current->next;
		}
		argc += args->integer;
	}

	// CALL
	return op3(self, CCALL, rpin(self), (intptr_t)call->fn, argc);
}

hot static inline int
emit_and(Compiler* self, Target* target, Ast* ast)
{
	int rresult;
	rresult = rpin(self);

	// lexpr
	// jntr _false

	// rexpr
	// jtr true

	// _false
		// set false
		// jmp end

	// true 
		// set true

	// _end
		// nop

	// lexpr
	int rl;
	rl = emit_expr(self, target, ast->l);

	// jntr _false
	int false_jmp = op_pos(self);
	op2(self, CJNTR, 0 /* _false */, rl);

	// rexpr
	int rr;
	rr = emit_expr(self, target, ast->r);

	// jtr true
	int true_jmp = op_pos(self);
	op2(self, CJTR, 0 /* true */, rr);

	// _false
	int _false = op_pos(self);
	op_set_jmp(self, false_jmp, _false);
	op2(self, CBOOL, rresult, 0);

	// jmp _end
	int end_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _end */);

	// true
	int rue = op_pos(self);
	op_set_jmp(self, true_jmp, rue);
	op2(self, CBOOL, rresult, 1);

	// _end
	int _end = op_pos(self);
	op_set_jmp(self, end_jmp, _end);
	op0(self, CNOP);

	runpin(self, rl);
	runpin(self, rr);
	return rresult;
}

hot static inline int
emit_or(Compiler* self, Target* target, Ast* ast)
{
	int rresult;
	rresult = rpin(self);

	// lexpr
		// jntr _next
		// jmp true

	// _next

	// rexpr
		// jntr _false
		// jmp true

	// _false
		// set false
		// jmp end

	// true
		// set true

	// _end
		// nop

	// lexpr
	int rl;
	rl = emit_expr(self, target, ast->l);

	// jntr _next
	int next_jmp = op_pos(self);
	op2(self, CJNTR, 0 /* _next */, rl);

	// jmp true
	int true_jmp0 = op_pos(self);
	op1(self, CJMP, 0 /* true */);

	// _next
	int _next = op_pos(self);
	op_set_jmp(self, next_jmp, _next);

	// rexpr
	int rr;
	rr = emit_expr(self, target, ast->r);

	// jntr _false
	int false_jmp = op_pos(self);
	op2(self, CJNTR, 0 /* _false */, rr);

	// jmp true
	int true_jmp1 = op_pos(self);
	op1(self, CJMP, 0 /* true */);

	// _false
	int _false = op_pos(self);
	op_set_jmp(self, false_jmp, _false);
	op2(self, CBOOL, rresult, 0);

	// jmp end
	int end_jmp = op_pos(self);
	op1(self, CJMP, 0 /* _end */);

	// true
	int rue = op_pos(self);
	op_set_jmp(self, true_jmp0, rue);
	op_set_jmp(self, true_jmp1, rue);
	op2(self, CBOOL, rresult, 1);

	// _end
	int _end = op_pos(self);
	op_set_jmp(self, end_jmp, _end);
	op0(self, CNOP);

	runpin(self, rl);
	runpin(self, rr);
	return rresult;
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
emit_cursor_idx(Compiler* self, Target* target, Str* path)
{
	int target_id = target->id;
	if (target->group_redirect)
		target_id = target->group_redirect->id;

	// copy path reference
	Str ref;
	str_set_str(&ref, path);

	// get name from the compound path
	//
	// name.path
	//
	Str  name;
	bool compound = str_split_or_set(&ref, &name, '.');

	// get target columns
	Columns* columns = NULL;
	if (target->cte)
		columns = &target->cte->columns;
	else
	if (target->view)
		columns = view_columns(target->view);
	else
	if (target->table)
		columns = table_columns(target->table);

	// find column in the target key
	int column_order = -1;
	if (columns)
	{
		// find column
		auto column = columns_find(columns, &name);
		if (column)
			column_order = column->order;
	} else
	{
		// try to resolve target expr labels
		if (target->expr && target->expr->id == KSELECT)
		{
			auto select = ast_select_of(target->expr);
			auto label  = ast_label_match(&select->expr_labels, &name);
			if (label)
				column_order = label->order;
		}
	}

	// exclude column or label name from the path
	if (column_order != -1)
	{
		// name.path
		// name
		// exclude name from the path
		if (compound)
			str_advance(&ref, str_size(&name) + 1);
		else
			str_init(&ref);
	}

	// current[name]
	// *.name
	int offset;
	if (! str_empty(&ref))
		offset = code_data_add_string(&self->code_data, &ref);
	else
		offset = -1;
	return op4(self, CCURSOR_IDX, rpin(self), target_id, column_order, offset);
}

hot static inline int
emit_name(Compiler* self, Target* target, Ast* ast)
{
	// SELECT name
	auto target_list = compiler_target_list(self);
	auto name = &ast->string;
	if (target_list->count == 0)
		error("<%.*s> name cannot be resolved without FROM clause",
		      str_size(name), str_of(name));

	if (unlikely(target == NULL))
		error("<%.*s> target column cannot be found",
		      str_size(name), str_of(name));

	// SELECT name FROM
	if (target_list->count == 1)
		return emit_cursor_idx(self, target, name);

	// GROUP BY / aggregation case
	if (target_is_join(target))
		error("<%.*s> path is ambiguous for JOIN",
		      str_size(name), str_of(name));

	// WHERE
	if (target->group_redirect == NULL)
	{
		// find lambda and read its state
		if (target->select)
		{
			// get lambda aggregate value using current group by key
			auto select = ast_select_of(target->select);
			auto lambda = ast_aggr_match(&select->expr_aggs, name);
			if (lambda)
			{
				// group by keys
				auto node = select->expr_group_by.list;
				while (node)
				{
					auto group = ast_group_of(node->ast);
					int rexpr = emit_expr(self, select->target, group->expr);
					op1(self, CPUSH, rexpr);
					runpin(self, rexpr);
					node = node->next;
				}
				return op3(self, CGROUP_GET, rpin(self), select->rgroup,
				           lambda->order);
			}
		}

		// read cursor
		return emit_cursor_idx(self, target, name);
	}

	// SELECT name GROUP BY
	// SELECT GROUP BY HAVING name

	// name must match
	//  - expression label
	//  - group by key label
	//  - group by key

	// use main target in case of group by target scan
	auto main = target;
	if (main->group_main)
		main = target->group_main;

	// match select label (aggregate or expression)
	if (main->select)
	{
		auto select = ast_select_of(main->select);
		auto label  = ast_label_match(&select->expr_labels, name);
		if (label)
			return emit_expr(self, target, label->expr);
	}

	// match group by key (group by key label)
	auto group = ast_group_match(&main->group_by, name);
	if (group == NULL)
		error("<%.*s> only GROUP BY keys can be selected",
		      str_size(name), str_of(name));

	return op3(self, CGROUP_READ, rpin(self), target->group_redirect->id,
	           group->order);
}

hot static inline int
emit_name_compound(Compiler* self, Target* target, Ast* ast)
{
	// check if first path is a table name
	Str name;
	str_split_or_set(&ast->string, &name, '.');

	Str path;
	str_init(&path);
	str_set_str(&path, &ast->string);

	auto target_list = compiler_target_list(self);
	if (target_list->count == 0)
		error("<%.*s> name cannot be resolved without FROM clause",
		      str_size(&name), str_of(&name));

	auto match = target_list_match(target_list, &name);
	if (match == NULL)
	{
		if (unlikely(! target))
			error("<%.*s> target cannot be found", str_size(&name),
			      str_of(&name));

		if (target_is_join(target))
			error("<%.*s> path is ambiguous for JOIN", str_size(&name),
			      str_of(&name));

		// use current target if table not found
		match = target;

		// use full path
	} else
	{
		// exclude table name from the path
		str_advance(&path, str_size(&name) + 1);
	}

	if (match->group_redirect == NULL)
		return emit_cursor_idx(self, match, &path);

	// GROUP BY / aggregation (group by target)

	// SELECT path GROUP BY
	// SELECT GROUP BY HAVING path

	// match group by list using the outer target
	auto outer = match->outer;
	auto group = ast_group_match(&outer->group_by, &ast->string);
	if (group == NULL)
		group = ast_group_match(&outer->group_by, &path);
	if (group == NULL)
		error("<%.*s> only GROUP BY keys can be selected",
		      str_size(&name), str_of(&name));
	return op3(self, CGROUP_READ, rpin(self), match->group_redirect->id,
	           group->order);
}

hot static inline int
emit_aggregate(Compiler* self, Target* target, Ast* ast)
{
	auto aggr = ast_aggr_of(ast);

	// target is target->group_target
	assert(target->group_redirect);

	// SELECT aggr GROUP BY
	// SELECT GROUP BY HAVING aggr

	// use main target id
	return op3(self, CGROUP_READ_AGGR, rpin(self), target->group_redirect->id,
	           aggr->order);
}

hot static inline int
emit_case(Compiler* self, Target* target, Ast* ast)
{
	auto cs = ast_case_of(ast);
	// CASE [expr] [WHEN expr THEN expr]
	//             [ELSE expr]
	// END
	int rresult = rpin(self);

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
			int rexpr = emit_expr(self, target, cs->expr);
			int rwhen = emit_expr(self, target, when->l);
			rcond = op3(self, CEQU, rpin(self), rexpr, rwhen);
			runpin(self, rexpr);
			runpin(self, rwhen);
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
		relse = emit_expr(self, target, cs->expr_else);
	else
		relse = op1(self, CNULL, rpin(self));
	op2(self, CSWAP, rresult, relse);
	runpin(self, relse);

	// _stop
	int _stop = op_pos(self);
	op_set_jmp(self, _stop_jmp, _stop);
	op0(self, CNOP);
	return rresult;
}

hot static inline int
emit_in(Compiler* self, Target* target, Ast* ast)
{
	auto expr = ast->l;
	auto args = ast->r;
	assert(args->id == KARGS);

	// expr
	int rresult = op2(self, CBOOL, rpin(self), false);

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
		int rin = op3(self, CIN, rpin(self), rexpr, r);
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
		rc = op2(self, CNOT, rpin(self), rresult);
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
		rc = op4(self, CANY, rpin(self), a, b, op);
		break;
	case KALL:
		rc = op4(self, CALL, rpin(self), a, b, op);
		break;
	default:
		abort();
		break;
	}
	runpin(self, a);
	runpin(self, b);
	return rc;
}

hot int
emit_expr(Compiler* self, Target* target, Ast* ast)
{
	switch (ast->id) {
	// logic
	case KOR:
		return emit_or(self, target, ast);
	case KAND:
		return emit_and(self, target, ast);

	// operators
	case '=':
		return emit_operator(self, target, ast, CEQU);
	case KNEQU:
		return emit_operator(self, target, ast, CNEQU);
	case KGTE:
		return emit_operator(self, target, ast, CGTE);
	case KLTE:
		return emit_operator(self, target, ast, CLTE);
	case '>':
		return emit_operator(self, target, ast, CGT);
	case '<':
		return emit_operator(self, target, ast, CLT);
	case '|':
		return emit_operator(self, target, ast, CBOR);
	case '^':
		return emit_operator(self, target, ast, CBXOR);
	case '&':
		return emit_operator(self, target, ast, CBAND);
	case KSHL:
		return emit_operator(self, target, ast, CSHL);
	case KSHR:
		return emit_operator(self, target, ast, CSHR);
	case '+':
		return emit_operator(self, target, ast, CADD);
	case '-':
		return emit_operator(self, target, ast, CSUB);
	case KCAT:
		return emit_operator(self, target, ast, CCAT);
	case '*':
		return emit_operator(self, target, ast, CMUL);
	case '/':
		return emit_operator(self, target, ast, CDIV);
	case '%':
		return emit_operator(self, target, ast, CMOD);
	case KBETWEEN:
		return emit_between(self, target, ast);

	// at
	case '.':
	case '[':
		return emit_operator(self, target, ast, CIDX);

	// unary operations
	case KNEG:
		return emit_unary(self, target, ast, CNEG);
	case KNOT:
		return emit_unary(self, target, ast, CNOT);

	// object
	case '{':
		return emit_obj(self, target, ast, CMAP);
	case KARRAY:
		return emit_obj(self, target, ast, CARRAY);

	// function/method call
	case KCALL:
		return emit_call(self, target, ast);
	case KMETHOD:
		return emit_call_method(self, target, ast);

	// consts
	case KNULL:
		return op1(self, CNULL, rpin(self));
	case KINT:
		return op2(self, CINT, rpin(self), ast->integer);
	case KREAL:
		return op2(self, CREAL, rpin(self), code_data_add_real(&self->code_data, ast->real));
	case KTRUE:
		return op2(self, CBOOL, rpin(self), 1);
	case KFALSE:
		return op2(self, CBOOL, rpin(self), 0);
	case KSTRING:
		return emit_string(self, &ast->string, ast->string_escape);
	case KINTERVAL:
	{
		int offset = code_data_offset(&self->code_data);
		encode_interval(&self->code_data.data, &ast->interval);
		return op2(self, CINTERVAL, rpin(self), offset);
	}
	case KTIMESTAMP:
		return op2(self, CTIMESTAMP, rpin(self), ast->integer);
	case KARGUMENT:
		return op2(self, CARG, rpin(self), ast->integer);

	// @
	case '@':
		if (target == NULL)
			error("<@> no FROM clause defined");
		return op2(self, CREF, rpin(self), target->id);

	// **
	case KSTAR_STAR:
		if (target == NULL)
			error("<**> no FROM clause defined");
		return op2(self, CREF_KEY, rpin(self), target->id);

	// name
	case KNAME:
		return emit_name(self, target, ast);

	case KNAME_COMPOUND:
		return emit_name_compound(self, target, ast);

	case KSTAR:
	{
		if (target == NULL)
			error("<*> no FROM clause defined");

		if (target_is_join(target))
			error("<*> path is ambiguous for JOIN");

		int target_id = target->id;
		if (target->group_redirect)
			target_id = target->group_redirect->id;

		return op2(self, CCURSOR_READ, rpin(self), target_id);
	}

	case KNAME_COMPOUND_STAR:
	{
		// path.*
		Str name;
		str_split_or_set(&ast->string, &name, '.');

		auto target_list = compiler_target_list(self);
		auto match = target_list_match(target_list, &name);
		if (match == NULL)
			error("<%.*s> target not found", str_size(&name), str_of(&name));

		int target_id = match->id;
		if (match->group_redirect)
			target_id = match->group_redirect->id;

		return op2(self, CCURSOR_READ, rpin(self), target_id);
	}

	case KNAME_COMPOUND_STAR_STAR:
	{
		// path.**
		Str name;
		str_split_or_set(&ast->string, &name, '.');

		auto target_list = compiler_target_list(self);
		auto match = target_list_match(target_list, &name);
		if (match == NULL)
			error("<%.*s> target not found", str_size(&name), str_of(&name));

		int target_id = match->id;
		if (match->group_redirect)
			target_id = match->group_redirect->id;

		return op2(self, CREF_KEY, rpin(self), target_id);
	}

	// sub-query
	case KSELECT:
		return emit_select(self, ast);

	// aggregate
	case KAGGR:
		return emit_aggregate(self, target, ast);

	// case
	case KCASE:
		return emit_case(self, target, ast);

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
		int r = emit_expr(self, target, ast->r);
		int rc = op2(self, CEXISTS, rpin(self), r);
		runpin(self, r);
		return rc;
	}

	// like
	case KLIKE:
	{
		int rexpr    = emit_expr(self, target, ast->l);
		int rpattern = emit_expr(self, target, ast->r);
		int rresult  = op3(self, CLIKE, rpin(self), rexpr, rpattern);
		runpin(self, rexpr);
		runpin(self, rpattern);

		// [not]
		if (! ast->integer)
		{
			int rc = op2(self, CNOT, rpin(self), rresult);
			runpin(self, rresult);
			rresult = rc;
		}
		return rresult;
	}

	default:
		assert(0);
		break;
	}

	assert(0);
	return -1;
}
