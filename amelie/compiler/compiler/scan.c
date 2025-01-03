
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
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

typedef struct
{
	Ast*         expr_where;
	int          roffset;
	int          rlimit;
	int          eof;
	ScanFunction on_match;
	void*        on_match_arg;
	Targets*     targets;
	Compiler*    compiler;
} Scan;

static inline int
scan_key(Scan* self, Target* target)
{
	auto cp    = self->compiler;
	auto plan  = target->plan;
	auto count = 0;

	list_foreach(&target->from_table_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &plan->keys[key->order];

		// use value from >, >=, = expression as a key
		if (! ref->start)
			break;
		int rexpr = emit_expr(cp, self->targets, ref->start);
		op1(cp, CPUSH, rexpr);
		runpin(cp, rexpr);
		count++;
	}

	return count;
}

static inline void
scan_stop(Scan* self, Target* target, int _eof)
{
	auto cp   = self->compiler;
	auto plan = target->plan;

	list_foreach(&target->from_table_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &plan->keys[key->order];
		if (! ref->stop)
			break;

		// use <, <= condition
		int rexpr;
		rexpr = emit_expr(cp, self->targets, ref->stop_op);

		// jntr _eof
		op2(cp, CJNTR, _eof, rexpr);
		runpin(cp, rexpr);
	}
}

static inline void
scan_target(Scan*, Target*);

static inline void
scan_table(Scan* self, Target* target)
{
	auto cp = self->compiler;
	auto table = target->from_table;

	auto plan = target->plan;
	auto point_lookup = (plan->type == PLAN_LOOKUP);
	auto index = target->from_table_index;

	// push cursor keys
	auto keys_count = scan_key(self, target);

	// save schema, table and index name
	int name_offset = code_data_offset(&cp->code_data);
	encode_string(&cp->code_data.data, &table->config->schema);
	encode_string(&cp->code_data.data, &table->config->name);
	encode_string(&cp->code_data.data, &index->name);

	// table_open
	int _open = op_pos(cp);
	int  open_op = CTABLE_OPEN;
	if (point_lookup)
		open_op = CTABLE_OPEN_LOOKUP;
	op4(cp, open_op, target->id, name_offset, 0 /* _where */, keys_count);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof (for limit)
	if (self->eof == -1)
		self->eof = _where_eof;

	// _where:
	int _where = op_pos(cp);
	code_at(cp->code, _open)->c = _where;

	// generate scan stop conditions for <, <= for tree index
	if (index->type == INDEX_TREE)
		scan_stop(self, target, _where_eof);

	if (target->next)
	{
		// recursive to inner target
		scan_target(self, target->next);
	} else
	{
		// generate inner target condition

		// where expr
		int _where_jntr;
		if (self->expr_where)
		{
			int rwhere = emit_expr(cp, self->targets, self->expr_where);
			_where_jntr = op_pos(cp);
			op2(cp, CJNTR, 0 /* _next */, rwhere);
			runpin(cp, rwhere);
		}

		// offset/limit counters
		int _offset_jmp;
		if (self->roffset != -1)
		{
			_offset_jmp = op_pos(cp);
			op2(cp, CJGTED, self->roffset, 0 /* _next */);
		}
		if (self->rlimit != -1)
			op2(cp, CJLTD, self->rlimit, self->eof);

		// aggregation / expr against current cursor position
		self->on_match(cp, self->targets, self->on_match_arg);

		// _next:
		int _next = op_pos(cp);
		if (self->expr_where)
			code_at(cp->code, _where_jntr)->a = _next;

		if (self->roffset != -1)
			code_at(cp->code, _offset_jmp)->b = _next;
	}

	// table_next

	// do not iterate further for point-lookups on unique index
	if (point_lookup && index->unique)
		op1(cp, CJMP, _where_eof);
	else
		op2(cp, CTABLE_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(cp->code, _where_eof)->a = _eof;
	op1(cp, CTABLE_CLOSE, target->id);
}

static inline void
scan_expr(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// emit expression, if not set
	switch (target->type) {
	case TARGET_SELECT:
	{
		if (target->r == -1)
			target->r = emit_select(cp, target->from_select);
		break;
	}
	case TARGET_CTE:
	{
		auto cte = target->from_cte;
		if (target->r == -1)
			target->r = op2(cp, CCTE_GET, rpin(cp, TYPE_STORE), cte->order);
		break;
	}
	case TARGET_FUNCTION:
	{
		if (target->r == -1)
			target->r = emit_call(cp, self->targets, target->from_function);
		break;
	}
	default:
		assert(target->r != -1);
		break;
	}

	// scan over set, union or function result
	int op_open  = CSTORE_OPEN;
	int op_close = CSTORE_CLOSE;
	int op_next  = CSTORE_NEXT;
	auto type = rtype(cp, target->r);
	if (type == TYPE_JSON)
	{
		op_open  = CJSON_OPEN;
		op_close = CJSON_CLOSE;
		op_next  = CJSON_NEXT;
	} else
	if (type != TYPE_STORE && type != TYPE_NULL) {
		error("FROM: unsupported expression type <%s>",
		      type_of(type));
	}

	// open SET, MERGE or JSON cursor
	auto _open = op_pos(cp);
	op3(cp, op_open, target->id, target->r, 0 /* _where */);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof (for limit)
	if (self->eof == -1)
		self->eof = _where_eof;

	// _where:
	int _where = op_pos(cp);
	code_at(cp->code, _open)->c = _where;

	if (target->next)
	{
		scan_target(self, target->next);
	} else
	{
		// where expr
		int _where_jntr;
		if (self->expr_where)
		{
			int rwhere = emit_expr(cp, self->targets, self->expr_where);
			_where_jntr = op_pos(cp);
			op2(cp, CJNTR, 0 /* _next */, rwhere);
			runpin(cp, rwhere);
		}

		// offset/limit counters
		int _offset_jmp;
		if (self->roffset != -1)
		{
			_offset_jmp = op_pos(cp);
			op2(cp, CJGTED, self->roffset, 0 /* _next */);
		}
		if (self->rlimit != -1)
			op2(cp, CJLTD, self->rlimit, self->eof);

		// aggregation / select_expr
		self->on_match(cp, self->targets, self->on_match_arg);

		// _next:
		int _next = op_pos(cp);
		if (self->expr_where)
			code_at(cp->code, _where_jntr)->a = _next;

		if (self->roffset != -1)
			code_at(cp->code, _offset_jmp)->b = _next;
	}

	// cursor next
	op2(cp, op_next, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(cp->code, _where_eof)->a = _eof;

	// cursor close
	op2(cp, op_close, target->id, true);
}

static inline void
scan_target(Scan* self, Target* target)
{
	if (target_is_table(target))
		scan_table(self, target);
	else
		scan_expr(self, target);
}

void
scan(Compiler*    compiler,
     Targets*     targets,
     Ast*         expr_limit,
     Ast*         expr_offset,
     Ast*         expr_where,
     ScanFunction on_match,
     void*        on_match_arg)
{
	Scan self =
	{
		.expr_where   = expr_where,
		.roffset      = -1,
		.rlimit       = -1,
		.eof          = -1,
		.on_match     = on_match,
		.on_match_arg = on_match_arg,
		.targets      = targets,
		.compiler     = compiler
	};

	// prepare scan plan using where expression per target
	planner(targets, expr_where);

	// offset
	if (expr_offset)
	{
		self.roffset = emit_expr(compiler, targets, expr_offset);
		if (rtype(compiler, self.roffset) != TYPE_INT)
			error("OFFSET: integer type expected");
	}

	// limit
	if (expr_limit)
	{
		self.rlimit = emit_expr(compiler, targets, expr_limit);
		if (rtype(compiler, self.rlimit) != TYPE_INT)
			error("LIMIT: integer type expected");
	}

	scan_target(&self, targets_outer(targets));

	if (self.roffset != -1)
		runpin(compiler, self.roffset);
	if (self.rlimit != -1)
		runpin(compiler, self.rlimit);
}
