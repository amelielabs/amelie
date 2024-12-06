
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
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

typedef struct
{
	Target*      target;
	Ast*         expr_where;
	int          roffset;
	int          rlimit;
	int          eof;
	ScanFunction on_match;
	void*        on_match_arg;
	Compiler*    compiler;
} Scan;

static inline void
scan_key(Scan* self, Target* target)
{
	auto cp   = self->compiler;
	auto path = ast_path_of(target->path);

	list_foreach(&target->from_table_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &path->keys[key->order];

		// use value from >, >=, = expression as a key
		if (ref->start)
		{
			int rexpr = emit_expr(cp, target, ref->start);
			op1(cp, CPUSH, rexpr);
			runpin(cp, rexpr);
			continue;
		}

		// set min
		int rexpr;
		switch (key->column->type) {
		case TYPE_INT:
		{
			if (key->column->type_size == 4)
				rexpr = op2(cp, CINT, rpin(cp, TYPE_INT), INT32_MIN);
			else
				rexpr = op2(cp, CINT, rpin(cp, TYPE_INT), INT64_MIN);
			break;
		}
		case TYPE_TIMESTAMP:
		{
			rexpr = op2(cp, CTIMESTAMP, rpin(cp, TYPE_TIMESTAMP), 0);
			break;
		}
		case TYPE_STRING:
		{
			Str empty;
			str_init(&empty);
			rexpr = emit_string(cp, &empty, false);
			break;
		}
		}
		op1(cp, CPUSH, rexpr);
		runpin(cp, rexpr);
	}
}

static inline void
scan_stop(Scan* self, Target* target, int _eof)
{
	auto cp   = self->compiler;
	auto path = ast_path_of(target->path);

	list_foreach(&target->from_table_index->keys.list)
	{
		auto key = list_at(Key, link);
		auto ref = &path->keys[key->order];
		if (! ref->stop)
			continue;

		// use <, <= condition
		int rexpr;
		rexpr = emit_expr(cp, target, ref->stop_op);

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
	auto target_list = compiler_target_list(cp);
	auto table = target->from_table;

	// prepare scan path using where expression per target
	planner(target_list, target, self->expr_where);
	auto path = ast_path_of(target->path);
	auto point_lookup = (path->type == PATH_LOOKUP);
	auto index = target->from_table_index;

	// push cursor keys
	scan_key(self, target);

	// save schema, table and index name
	int name_offset = code_data_offset(&cp->code_data);
	encode_string(&cp->code_data.data, &table->config->schema);
	encode_string(&cp->code_data.data, &table->config->name);
	encode_string(&cp->code_data.data, &index->name);

	// table_open
	int _open = op_pos(cp);
	op4(cp, CTABLE_OPEN, target->id, name_offset, 0 /* _where */, point_lookup);

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

	if (target->next_join)
	{
		// recursive to inner target
		scan_target(self, target->next_join);
	} else
	{
		// generate inner target condition

		// where expr
		int _where_jntr;
		if (self->expr_where)
		{
			int rwhere = emit_expr(cp, target, self->expr_where);
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
		self->on_match(cp, self->on_match_arg);

		// _next:
		int _next = op_pos(cp);
		if (self->expr_where)
		{
			// point lookup
			if (point_lookup)
				code_at(cp->code, _where_jntr)->a = _where_eof;
			else
				code_at(cp->code, _where_jntr)->a = _next;
		}
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
scan_store(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// scan over SET or MERGE object
	//
	// using target register value to determine the scan object
	if (target->r == -1)
	{
		if (target->type == TARGET_SELECT)
		{
			assert(target->from_select);
			target->r = emit_select(cp, target->from_select);
		} else
		if (target->type == TARGET_CTE)
		{
			auto cte = target->from_cte;
			target->r = op2(cp, CCTE_GET, rpin(cp, TYPE_STORE), cte->stmt);
		} else {
			assert(false);
		}
	}

	// open SET or MERGE cursor
	auto _open = op_pos(cp);
	op3(cp, CSTORE_OPEN, target->id, target->r, 0 /* _where */);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof (for limit)
	if (self->eof == -1)
		self->eof = _where_eof;

	// _where:
	int _where = op_pos(cp);
	code_at(cp->code, _open)->c = _where;

	if (target->next_join)
	{
		scan_target(self, target->next_join);
	} else
	{
		// where expr
		int _where_jntr;
		if (self->expr_where)
		{
			int rwhere = emit_expr(cp, target, self->expr_where);
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
		self->on_match(cp, self->on_match_arg);

		// _next:
		int _next = op_pos(cp);
		if (self->expr_where)
			code_at(cp->code, _where_jntr)->a = _next;

		if (self->roffset != -1)
			code_at(cp->code, _offset_jmp)->b = _next;
	}

	// cursor next
	op2(cp, CSTORE_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(cp->code, _where_eof)->a = _eof;

	// STORE_CLOSE
	op2(cp, CSTORE_CLOSE, target->id, true);
}

static inline void
scan_target(Scan* self, Target* target)
{
	if (target->type == TARGET_TABLE ||
	    target->type == TARGET_TABLE_SHARED)
		scan_table(self, target);
	else
		scan_store(self, target);
}

void
scan(Compiler*    compiler,
     Target*      target,
     Ast*         expr_limit,
     Ast*         expr_offset,
     Ast*         expr_where,
     ScanFunction on_match,
     void*        on_match_arg)
{
	Scan self =
	{
		.target       = target,
		.expr_where   = expr_where,
		.roffset      = -1,
		.rlimit       = -1,
		.eof          = -1,
		.on_match     = on_match,
		.on_match_arg = on_match_arg,
		.compiler     = compiler
	};

	// offset
	if (expr_offset)
	{
		self.roffset = emit_expr(compiler, target, expr_offset);
		if (rtype(compiler, self.roffset) != TYPE_INT)
			error("OFFSET: integer type expected");
	}

	// limit
	if (expr_limit)
	{
		self.rlimit = emit_expr(compiler, target, expr_limit);
		if (rtype(compiler, self.rlimit) != TYPE_INT)
			error("LIMIT: integer type expected");
	}

	scan_target(&self, target);

	if (self.roffset != -1)
		runpin(compiler, self.roffset);
	if (self.rlimit != -1)
		runpin(compiler, self.rlimit);
}
