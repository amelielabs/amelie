
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
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

typedef struct
{
	Target*      target;
	Ast*         expr_limit;
	Ast*         expr_offset;
	Ast*         expr_where;
	int          coffset;
	int          climit;
	int          climit_eof;
	ScanFunction on_match;
	void*        on_match_arg;
	Compiler*    compiler;
} Scan;

static inline void
scan_key(Scan* self, Target* target)
{
	auto cp   = self->compiler;
	auto path = ast_path_of(target->path);

	list_foreach(&target->index->keys.list)
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
		switch (key->type) {
		case TYPE_INT:
			rexpr = op1(cp, CINT_MIN, rpin(cp));
			break;
		case TYPE_TIMESTAMP:
			rexpr = op1(cp, CTIMESTAMP_MIN, rpin(cp));
			break;
		case TYPE_STRING:
			rexpr = op1(cp, CSTRING_MIN, rpin(cp));
			break;
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

	list_foreach(&target->index->keys.list)
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
scan_target_table(Scan* self, Target* target)
{
	auto cp = self->compiler;
	auto target_list = compiler_target_list(cp);
	auto table = target->table;

	// prepare scan path using where expression per target
	planner(target_list, target, self->expr_where);
	auto path = ast_path_of(target->path);
	auto point_lookup = (path->type == PATH_LOOKUP);
	auto index = target->index;

	// push cursor keys
	scan_key(self, target);

	// save schema, table and index name
	int name_offset = code_data_offset(&cp->code_data);
	encode_string(&cp->code_data.data, &table->config->schema);
	encode_string(&cp->code_data.data, &table->config->name);
	encode_string(&cp->code_data.data, &index->name);

	// cursor_open
	int _open = op_pos(cp);
	op4(cp, CCURSOR_OPEN, target->id, name_offset, 0 /* _where */, point_lookup);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof for limit expression
	if (self->climit_eof == -1)
		self->climit_eof = _where_eof;

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
		int _coffset_jmp;
		if (self->expr_offset)
		{
			_coffset_jmp = op_pos(cp);
			op3(cp, CCNTR_GTE, target->id, 1 /* type */, 0 /* _next */);
		}
		if (self->expr_limit)
			op3(cp, CCNTR_LTE, target->id, 0 /* type */, self->climit_eof);

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
		if (self->expr_offset)
			code_at(cp->code, _coffset_jmp)->c = _next;
	}

	// cursor_next

	// do not iterate further for point-lookups on unique index
	if (point_lookup && index->unique)
		op1(cp, CJMP, _where_eof);
	else
		op2(cp, CCURSOR_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(cp->code, _where_eof)->a = _eof;
	op1(cp, CCURSOR_CLOSE, target->id);
}

static inline void
scan_target_expr(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// use expression or CTE result for open
	int _open;
	int rexpr = -1;
	if (target->cte)
	{
		// cursor_open_cte
		_open = op_pos(cp);
		op3(cp, CCURSOR_OPEN_CTE, target->id, target->cte->order, 0 /* _where */);
	} else
	{
		// expr
		if (target->rexpr != -1)
			rexpr = target->rexpr;
		else
			rexpr = emit_expr(cp, target, target->expr);
		// cursor_open_expr
		_open = op_pos(cp);
		op3(cp, CCURSOR_OPEN_EXPR, target->id, rexpr, 0 /* _where */);
	}

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof for limit expression
	if (self->climit_eof == -1)
		self->climit_eof = _where_eof;

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
		int _coffset_jmp;
		if (self->expr_offset)
		{
			_coffset_jmp = op_pos(cp);
			op3(cp, CCNTR_GTE, target->id, 1 /* type */, 0 /* _next */);
		}
		if (self->expr_limit)
			op3(cp, CCNTR_LTE, target->id, 0 /* type */, self->climit_eof);

		// aggregation / select_expr
		self->on_match(cp, self->on_match_arg);

		// _next:
		int _next = op_pos(cp);
		if (self->expr_where)
			code_at(cp->code, _where_jntr)->a = _next;

		if (self->expr_offset)
			code_at(cp->code, _coffset_jmp)->c = _next;
	}

	// cursor_next
	op2(cp, CCURSOR_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(cp->code, _where_eof)->a = _eof;
	op1(cp, CCURSOR_CLOSE, target->id);
	if (rexpr != -1)
		runpin(cp, rexpr);
}

static inline void
scan_target(Scan* self, Target* target)
{
	if (target->table)
		scan_target_table(self, target);
	else
		scan_target_expr(self, target);
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
		.expr_limit   = expr_limit,
		.expr_offset  = expr_offset,
		.expr_where   = expr_where,
		.coffset      = -1,
		.climit       = -1,
		.climit_eof   = -1,
		.on_match     = on_match,
		.on_match_arg = on_match_arg,
		.compiler     = compiler
	};

	// offset
	if (expr_offset)
	{
		int roffset;
		roffset = emit_expr(compiler, target, expr_offset);
		self.coffset = op_pos(compiler);
		op3(compiler, CCNTR_INIT, target->id, 1, roffset);
		runpin(compiler, roffset);
	}

	// limit
	if (expr_limit)
	{
		int rlimit;
		rlimit = emit_expr(compiler, target, expr_limit);
		self.climit = op_pos(compiler);
		op3(compiler, CCNTR_INIT, target->id, 0, rlimit);
		runpin(compiler, rlimit);
	}

	scan_target(&self, target);
}
