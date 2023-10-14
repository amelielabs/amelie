
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

static inline int
emit_string_as(Compiler* self, bool escape, Str* string)
{
	int offset;
	if (escape)
		offset = code_add_string(self->code, string);
	else
		offset = code_add_string_unescape(self->code, string);
	return op2(self, CSTRING, rpin(self), offset);
}

static inline int
emit_string(Compiler* self, Ast* ast)
{
	assert(ast->id == KSTRING ||
	       ast->id == KNAME   ||
	       ast->id == KNAME_COMPOUND);
	return emit_string_as(self, ast->string_escape, &ast->string);
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
emit_call(Compiler* self, Target* target, Ast* ast, int op)
{
	auto call = ast->l;
	assert(call->id == KCALL);

	// push arguments
	auto current = call->l;
	while (current)
	{
		int r = emit_expr(self, target, current);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// op
	return op2(self, op, rpin(self), call->integer);
}

hot static inline int
emit_call_function(Compiler* self, Target* target, Ast* ast)
{
	// (function_name, call)

	// call function by name
	auto name = ast->l;
	auto call = ast->r;
	assert(call->id == KCALL);

	// push arguments
	auto current = call->l;
	while (current)
	{
		int r = emit_expr(self, target, current);
		op1(self, CPUSH, r);
		runpin(self, r);
		current = current->next;
	}

	// call
	int name_offset;
	name_offset = code_add_string(self->code, &name->string);
	return op3(self, CCALL, rpin(self), name_offset, call->integer);
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
emit_cursor_idx(Compiler* self, Target* target, Str* path)
{
	int target_id = target->id;
	if (target->group_redirect)
		target_id = target->group_redirect->id;

	// copy path reference
	Str ref;
	str_set_str(&ref, path);

	// find column in the target schema
	int column_order = -1;
	if (target->table)
	{
		// get column name from the compound path
		//
		// column.path
		//
		Str  name;
		bool compound = str_split_or_set(&ref, &name, '.');

		// find column or column reference
		auto schema = table_schema(target->table);
		auto column = schema_find_column(schema, &name);
		if (column)
		{
			column_order = column->order;
		} else
		{
			// todo: find reference
		}

		// exclude column name from the path
		if (column_order != -1)
		{
			// column.path
			// column
			if (compound)
			{
				// exclude column name from the path
				str_advance(&ref, str_size(&name) + 1);
			} else 
			{
				// path is empty, using column order
				str_init(&ref);
			}
		}
	}

	// current[name]
	// *.name
	int offset;
	if (! str_empty(&ref))
		offset = code_add_string(self->code, &ref);
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

	// SELECT name FROM
	if (target_list->count == 1)
		return emit_cursor_idx(self, target, name);

	// match aggregate label first
	auto aggr = ast_aggr_match(&self->current->aggr_list, name);
	if (aggr)
	{
		// WHERE aggr_label
		assert(aggr->target);
		if (unlikely(aggr->target->group_redirect == NULL))
			error("<%.*s> aggregate target is out of scope",
			      str_size(name), str_of(name));

		return op3(self, CGROUP_GET_AGGR,
		           rpin(self),
		           aggr->target->group_redirect->id,
		           aggr->order);
	}

	// GROUP BY / aggregation case
	if (target_is_join(target))
		error("<%.*s> path is ambiguous for JOIN",
		      str_size(name), str_of(name));

	// WHERE
	if (target->group_redirect == NULL)
		return emit_cursor_idx(self, target, name);

	// SELECT or HAVING (group by target)

	// name must match one of the group by keys

	// use main target in case of group by target scan
	auto main = target;
	if (main->group_main)
		main = target->group_main;

	auto group = ast_group_match(&main->group_by, name);
	if (group == NULL)
		error("<%.*s> only GROUP BY keys can be selected",
		      str_size(name), str_of(name));

	return op3(self, CGROUP_GET, rpin(self), target->group_redirect->id,
	           group->order);
}

hot static inline int
emit_name_compound(Compiler* self, Target* target, Ast* ast)
{
	// check if first path is a table name
	Str name;
	str_split_or_set(&name, &ast->string, '.');

	Str path;
	str_init(&path);

	auto target_list = compiler_target_list(self);
	if (target_list->count == 0)
		error("<%.*s> name cannot be resolved without FROM clause",
		      str_size(&name), str_of(&name));

	auto match = target_list_match(target_list, &name);
	if (match == NULL)
	{
		if (target_is_join(target))
			error("<%.*s> path is ambiguous for JOIN",
			      str_size(&name), str_of(&name));

		// use current target if table not found
		match = target;

		// use full path
		str_set_str(&path, &ast->string);
	} else
	{
		// exclude table name from the path
		str_advance(&path, str_size(&name) + 1);
	}

	if (match->group_redirect == NULL)
		return emit_cursor_idx(self, match, &path);

	// GROUP BY / aggregation 

	// SELECT or HAVING (group by target)

	// match group by list using the outer target
	auto outer = match->outer;
	auto group = ast_group_match(&outer->group_by, &ast->string);
	if (group == NULL)
		group = ast_group_match(&outer->group_by, &path);
	if (group == NULL)
		error("<%.*s> only GROUP BY keys can be selected",
		      str_size(&name), str_of(&name));
	return op3(self, CGROUP_GET, rpin(self), match->group_redirect->id,
	           group->order);
}

hot static inline int
emit_aggregate(Compiler* self, Ast* ast)
{
	auto aggr = ast_aggr_of(ast);

	// partial aggregate
#if 0
	if (aggr->target == NULL)
	{
		if (ast->token.type != KCOUNT)
			cmd_error(self, "unknown partial aggregate");

		int type = PARTIAL_COUNT;

		int rexpr;
		rexpr = emit(self, NULL, aggr->expr);

		int rpartial;
		rpartial = op3(self, CPARTIAL, rpin(self), type, rexpr);
		runpin(self, rexpr);
		return rpartial;
	}
#endif

	auto target = aggr->target;
	assert(target != NULL);
	return op3(self, CGROUP_GET_AGGR, rpin(self), target->group_redirect->id,
	           aggr->order);
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
	case '[':
		return emit_operator(self, target, ast, CIDX);

	// unary operations
	case KNEG:
		return emit_unary(self, target, ast, CNEG);
	case KNOT:
		return emit_unary(self, target, ast, CNOT);

	// object
	case '{':
		return emit_call(self, target, ast, CMAP);
	case KARRAY:
		return emit_call(self, target, ast, CARRAY);

	// function call
	case '(':
		return emit_call_function(self, target, ast);

#if 0
	// object ops
	case KSET:
		return emit_call(self, target, ast, COBJ_SET);
	case KUNSET:
		return emit_call(self, target, ast, COBJ_UNSET);
	case KHAS:
		return emit_call(self, target, ast, COBJ_HAS);
	case KTSTRING:
		return emit_call(self, target, ast, CTO_STRING);
	case KJSON:
		return emit_call(self, target, ast, CTO_JSON);
	case KSIZEOF:
		return emit_call(self, target, ast, CSIZEOF);
#endif

	// consts
	case KNULL:
		return op1(self, CNULL, rpin(self));
	case KINT:
		return op2(self, CINT, rpin(self), ast->integer);
	case KFLOAT:
		return op2(self, CFLOAT, rpin(self), code_add_fp(self->code, ast->fp));
	case KTRUE:
		return op2(self, CBOOL, rpin(self), 1);
	case KFALSE:
		return op2(self, CBOOL, rpin(self), 0);
	case KSTRING:
		return emit_string(self, ast);
	case KARGUMENT:
		return op2(self, CARG, rpin(self), ast->integer);
	case KARGUMENT_NAME:
	{
#if 0
		if (unlikely(self->args == NULL))
			cmd_error(self, "no arguments defined");

		// find argument by name
		auto column = schema_find_column(self->args, &ast->string);
		if (column)
			return op2(self, CARG_NAME, rpin(self), column->order);

		cmd_error(self, "<%.*s> argument not found", str_of(&ast->tstring),
		          str_size(&ast->string));
#endif
		break;
	}

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
		str_split_or_set(&name, &ast->string, '.');

		auto target_list = compiler_target_list(self);
		auto match = target_list_match(target_list, &name);
		if (match == NULL)
			error("<%.*s> target not found", str_size(&name), str_of(&name));

		int target_id = match->id;
		if (match->group_redirect)
			target_id = match->group_redirect->id;

		return op2(self, CCURSOR_READ, rpin(self), target_id);
	}

	// sub-query
	case KSELECT:
		return emit_select(self, ast, true);

	// aggregates
	case KCOUNT:
	case KSUM:
	case KAVG:
		return emit_aggregate(self, ast);

	default:
		assert(0);
		break;
	}

	assert(0);
	return -1;
}
