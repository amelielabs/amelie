
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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
	auto path  = target->path;
	auto count = 0;

	list_foreach(&target->from_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &path->keys[key->order];

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
scan_stop(Scan* self, Target* target, int scan_stop_jntr[])
{
	auto cp   = self->compiler;
	auto path = target->path;

	list_foreach(&target->from_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &path->keys[key->order];
		if (! ref->stop)
			break;

		// use <, <= condition
		int rexpr;
		rexpr = emit_expr(cp, self->targets, ref->stop_op);

		// jntr _eof
		scan_stop_jntr[key->order] = op_pos(cp);
		op2(cp, CJNTR, 0 /* _eof */, rexpr);
		runpin(cp, rexpr);
	}
}

static inline void
scan_on_match(Scan* self)
{
	// generate on match condition
	auto cp = self->compiler;

	// where expr
	int _where_jntr = -1;
	if (self->expr_where)
	{
		int rwhere = emit_expr(cp, self->targets, self->expr_where);
		_where_jntr = op_pos(cp);
		op2(cp, CJNTR, 0 /* _next */, rwhere);
		runpin(cp, rwhere);
	}

	// offset/limit counters
	int _offset_jmp = -1;
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

static inline void
scan_target(Scan*, Target*);

static inline void
scan_table(Scan* self, Target* target)
{
	auto cp = self->compiler;
	auto table = target->from_table;
	auto index = target->from_index;
	auto path  = target->path;
	auto point_lookup = (path->type == PATH_LOOKUP);

	// save schema, table and index name
	auto name_offset = code_data_offset(cp->code_data);
	auto data = &cp->code_data->data;
	encode_string(data, &table->config->schema);
	encode_string(data, &table->config->name);
	encode_string(data, &index->name);

	// push cursor keys
	auto keys_count = scan_key(self, target);

	// table_open (open a single partition or do table scan)
	int _open = op_pos(cp);
	int  open_op;
	if (target->from_access == ACCESS_RO_EXCLUSIVE)
	{
		// subquery or inner join target
		if (point_lookup)
			open_op = CTABLE_OPENL;
		else
			open_op = CTABLE_OPEN;
	} else
	{
		if (point_lookup)
			open_op = CTABLE_OPEN_PARTL;
		else
			open_op = CTABLE_OPEN_PART;
	}

	// create table cursor
	target->rcursor = op4(cp, open_op, rpin(cp, TYPE_CURSOR),
	                      name_offset, 0 /* _eof */,
	                      keys_count);

	// handle outer target eof jmp (for limit)
	if (self->rlimit != -1 && !target->prev)
	{
		// jmp to _where
		int _start = op_pos(cp);
		op1(cp, CJMP, 0 /* _where */);

		// jmp to _eof
		self->eof = op_pos(cp);
		op1(cp, CJMP, 0 /* _eof */);

		code_at(cp->code, _start)->a = op_pos(cp);
	}

	// _where:
	int _where = op_pos(cp);

	// generate scan stop conditions for <, <= for tree index
	int scan_stop_jntr[path->match_stop];
	if (index->type == INDEX_TREE)
		scan_stop(self, target, scan_stop_jntr);

	if (target->next)
		scan_target(self, target->next);
	else
		scan_on_match(self);

	// table_next

	// do not iterate further for point-lookups on unique index
	if (! (point_lookup && index->unique))
		op2(cp, CTABLE_NEXT, target->rcursor, _where);

	// _eof:
	int _eof = op_pos(cp);

	// set table_open to _eof
	code_at(cp->code, _open)->c = _eof;

	// set scan stop jntr to eof
	for (auto order = 0; order < path->match_stop; order++)
		code_at(cp->code, scan_stop_jntr[order])->a = _eof;

	// set outer target eof jmp for limit
	if (self->rlimit != -1 && !target->prev)
		code_at(cp->code, self->eof)->a = _eof;

	// close cursor
	op1(cp, CFREE, target->rcursor);
	runpin(cp, target->rcursor);
	target->rcursor = -1;
}

static inline void
scan_table_heap(Scan* self, Target* target)
{
	auto cp    = self->compiler;
	auto table = target->from_table;

	// save schema, table (no index)
	auto name_offset = code_data_offset(cp->code_data);
	auto data = &cp->code_data->data;
	encode_string(data, &table->config->schema);
	encode_string(data, &table->config->name);

	// ensure target does not require full table access
	if (unlikely(target->from_access == ACCESS_RO_EXCLUSIVE))
		error("heap only scan for subqueries and inner join targets is not supported");

	// table_open
	int _open = op_pos(cp);
	target->rcursor = op4(cp, CTABLE_OPEN_HEAP, rpin(cp, TYPE_CURSOR),
	                      name_offset, 0 /* _eof */, 0);

	// handle outer target eof jmp (for limit)
	if (self->rlimit != -1 && !target->prev)
	{
		// jmp to _where
		int _start = op_pos(cp);
		op1(cp, CJMP, 0 /* _where */);

		// jmp to _eof
		self->eof = op_pos(cp);
		op1(cp, CJMP, 0 /* _eof */);

		code_at(cp->code, _start)->a = op_pos(cp);
	}

	// _where:
	int _where = op_pos(cp);
	if (target->next)
		scan_target(self, target->next);
	else
		scan_on_match(self);

	// table_next
	op2(cp, CTABLE_NEXT, target->rcursor, _where);

	// _eof:
	int _eof = op_pos(cp);

	// set table_open to _eof
	code_at(cp->code, _open)->c = _eof;

	// set outer target eof jmp for limit
	if (self->rlimit != -1 && !target->prev)
		code_at(cp->code, self->eof)->a = _eof;

	// close cursor
	op1(cp, CFREE, target->rcursor);
	runpin(cp, target->rcursor);
	target->rcursor = -1;
}

static inline void
scan_expr(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// emit expression, if not set
	switch (target->type) {
	case TARGET_EXPR:
	case TARGET_FUNCTION:
	{
		if (target->r == -1)
		{
			auto r = emit_expr(cp, self->targets, target->ast);
			auto rt = rtype(cp, r);
			if (rt == TYPE_STORE || rt == TYPE_JSON || rt == TYPE_NULL) {
				target->r = r;
			} else
			{
				// wrap result into set
				op1(cp, CPUSH, r);
				runpin(cp, r);
				target->r = op3(cp, CSET, rpin(cp, TYPE_STORE), 1, 0);
				op1(cp, CSET_ADD, target->r);
			}

			// set expression type
			assert(target->columns->count == 1);
			column_set_type(columns_first(target->columns), rt, type_sizeof(rt));
		}
		break;
	}
	case TARGET_CTE:
	case TARGET_STMT:
	{
		auto stmt = target->from_stmt;
		if (target->r == -1)
		{
			assert(stmt->r != -1);
			auto rt = rtype(cp, stmt->r);
			auto r  = op2(cp, CREF, rpin(cp, rt), stmt->r);
			if (rt == TYPE_STORE || rt == TYPE_JSON || rt == TYPE_NULL) {
				target->r = r;
			} else
			{
				// wrap result into set
				op1(cp, CPUSH, r);
				runpin(cp, r);
				target->r = op3(cp, CSET, rpin(cp, TYPE_STORE), 1, 0);
				op1(cp, CSET_ADD, target->r);
			}
		}
		break;
	}
	case TARGET_VAR:
	{
		auto var = target->from_var;
		if (target->r == -1)
		{
			auto rt = rtype(cp, var->r);
			auto r  = op2(cp, CREF, rpin(cp, rt), var->r);
			if (rt == TYPE_STORE || rt == TYPE_JSON || rt == TYPE_NULL) {
				target->r = r;
			} else
			{
				// wrap result into set
				op1(cp, CPUSH, r);
				runpin(cp, r);
				target->r = op3(cp, CSET, rpin(cp, TYPE_STORE), 1, 0);
				op1(cp, CSET_ADD, target->r);
			}
		}
		break;
	}
	default:
		break;
	}
	assert(target->r != -1);

	// scan over set, union or function result
	int cursor_open = CSTORE_OPEN;
	int cursor_next = CSTORE_NEXT;
	int cursor_type = TYPE_CURSOR_STORE;

	auto type = rtype(cp, target->r);
	if (type == TYPE_JSON)
	{
		cursor_open = CJSON_OPEN;
		cursor_next = CJSON_NEXT;
		cursor_type = TYPE_CURSOR_JSON;
	} else
	if (type != TYPE_STORE && type != TYPE_NULL) {
		stmt_error(cp->current, target->ast, "unsupported expression type '%s'",
		           type_of(type));
	}

	// open SET/MERGE or JSON cursor
	auto _open = op_pos(cp);
	target->rcursor = op3(cp, cursor_open, rpin(cp, cursor_type),
	                      target->r, 0 /* _eof */);

	// handle outer target eof jmp (for limit)
	if (self->rlimit != -1 && !target->prev)
	{
		// jmp to _where
		int _start = op_pos(cp);
		op1(cp, CJMP, 0 /* _where */);

		// jmp to _eof
		self->eof = op_pos(cp);
		op1(cp, CJMP, 0 /* _eof */);

		code_at(cp->code, _start)->a = op_pos(cp);
	}

	// _where:
	int _where = op_pos(cp);
	if (target->next)
		scan_target(self, target->next);
	else
		scan_on_match(self);

	// cursor next
	op2(cp, cursor_next, target->rcursor, _where);

	// _eof:
	int _eof = op_pos(cp);

	// set open to _eof
	code_at(cp->code, _open)->c = _eof;

	// set outer target eof jmp for limit
	if (self->rlimit != -1 && !target->prev)
		code_at(cp->code, self->eof)->a = _eof;

	// close cursor
	op1(cp, CFREE, target->rcursor);
	runpin(cp, target->rcursor);
	target->rcursor = -1;

	// free target value
	op1(cp, CFREE, target->r);
	runpin(cp, target->r);
	target->r = -1;
}

static inline void
scan_target(Scan* self, Target* target)
{
	if (target_is_table(target))
	{
		if (target->from_heap)
			scan_table_heap(self, target);
		else
			scan_table(self, target);
	} else {
		scan_expr(self, target);
	}
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

	// prepare scan path using where expression per target
	path_prepare(targets, expr_where);

	// limit
	if (expr_limit)
		self.rlimit = emit_expr(compiler, targets, expr_limit);

	// offset
	if (expr_offset)
		self.roffset = emit_expr(compiler, targets, expr_offset);

	scan_target(&self, targets_outer(targets));

	if (self.roffset != -1)
		runpin(compiler, self.roffset);
	if (self.rlimit != -1)
		runpin(compiler, self.rlimit);
}
