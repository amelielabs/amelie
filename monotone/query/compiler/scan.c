
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
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>

typedef struct
{
	Target*      target;
	Ast*         expr_limit;
	Ast*         expr_offset;
	Ast*         expr_where;
	AstList      ops;
	int          coffset;
	int          climit;
	int          climit_eof;
	ScanFunction on_match_point;
	ScanFunction on_match;
	void*        on_match_arg;
	Compiler*    compiler;
} Scan;

static inline bool
scan_key_is_match(Target* target, Ast* ast, Column* key)
{
	// column
	if (ast->id == KNAME)
		if (str_compare(&ast->string, &key->name))
			return true;

	// target.column
	if (ast->id == KNAME_COMPOUND)
	{
		Str name;
		str_split_or_set(&name, &ast->string, '.');
		if (target_compare(target, &name))
		{
			// match key path
			Str path;
			str_set_str(&path, &ast->string);
			str_advance(&path, str_size(&name) + 1);
			if (str_compare(&path, &key->name))
				return true;
		}
	}

	return false;
}

static inline Ast*
scan_key_match(Scan* self, Target* target, Ast* op, Column* key)
{
	auto l = op->l;
	auto r = op->r;

	//
	// name = expr
	// expr = name
	//
	Ast* expr_key = NULL;
	Ast* expr = NULL;
	if (scan_key_is_match(target, l, key))
	{
		expr_key = l;
		expr     = r;
	} else
	if (scan_key_is_match(target, r, key))
	{
		expr_key = r;
		expr     = l;
	}

	// join: name = name
	if (expr_key)
	{
		// do not match keys for outer targets
		if (expr->id == KNAME_COMPOUND)
		{
			Str name;
			str_split_or_set(&name, &expr->string, '.');

			auto target_list = compiler_target_list(self->compiler);
			auto join = target_list_match(target_list, &name);
			if (join)
			{
				if (join == target)
					return NULL;

				// FROM target, join
				if (target->level == join->level)
					if (target->level_seq < join->level_seq)
						return NULL;

				// SELECT (SELECT FROM join) FROM target
				if (target->level < join->level)
					return NULL;

				// SELECT (SELECT FROM target) FROM join
			}
		}
	}

	return expr;
}

static inline void
scan_analyze(Scan* self, Target* target)
{
	int key_matched_stop = 0;
	int key_matched_equ  = 0;
	int key_matched      = 0;

	auto schema = table_schema(target->table);
	auto key = schema->key;
	for (; key; key = key->next_key)
	{
		auto node = self->ops.list;
		for (; node; node = node->next)
		{
			auto expr = scan_key_match(self, target, node->ast, key);
			if (expr == NULL)
				continue;
			switch (node->ast->id) {
			case '>':
			case KGTE:
				key_matched++;
				break;
			case '=':
				key_matched_equ++;
				key_matched++;
				break;
			case '<':
			case KLTE:
				key_matched_stop++;
				break;
			default:
				continue;
			}
		}
	}

	// point lookup
	if (key_matched_equ == schema->key_count)
		target->is_point_lookup = true;

	// has scan stop expressions <, <=
	if (key_matched_stop > 0)
		target->stop_ops = true;
}

static inline void
scan_generate_key(Scan* self, Target* target)
{
	auto cp = self->compiler;
	auto schema = table_schema(target->table);

	auto key = schema->key;
	for (; key; key = key->next_key)
	{
		bool match = false;
		auto node = self->ops.list;
		for (; node; node = node->next)
		{
			auto expr = scan_key_match(self, target, node->ast, key);
			if (expr == NULL)
				continue;
			switch (node->ast->id) {
			case '>':
			case KGTE:
			case '=':
				break;
			case '<':
			case KLTE:
				continue;
			case 0:
				continue;
			}

			int rexpr = emit_expr(cp, target, expr);
			op1(cp, CPUSH, rexpr);
			runpin(cp, rexpr);

			match = true;
			break;
		}

		// set min
		if (unlikely(! match))
		{
			int rexpr;
			if (key->type == TYPE_STRING)
				rexpr = op1(cp, CSTRING_MIN, rpin(cp));
			else
				rexpr = op1(cp, CINT_MIN, rpin(cp));
			op1(cp, CPUSH, rexpr);
			runpin(cp, rexpr);
		}
	}
}

static inline void
scan_generate_stop(Scan* self, Target* target, int _eof)
{
	auto cp = self->compiler;
	auto schema = table_schema(target->table);

	auto key = schema->key;
	for (; key; key = key->next_key)
	{
		auto node = self->ops.list;
		for (; node; node = node->next)
		{
			auto expr = scan_key_match(self, target, node->ast, key);
			if (expr == NULL)
				continue;
			switch (node->ast->id) {
			case '<':
			case KLTE:
				break;
			default:
				continue;
			}

			// key <= expr
			int rexpr;
			rexpr = emit_expr(cp, target, node->ast);

			// jntr _eof
			op2(cp, CJNTR, _eof, rexpr);
			runpin(cp, rexpr);
		}
	}
}

static inline void
scan_generate_target(Scan*, Target*);

static inline void
scan_generate_target_table(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// analyze where expression per target
	scan_analyze(self, target);

	// push cursor keys
	scan_generate_key(self, target);

	// do fast point lookup path if supported
	auto target_list = compiler_target_list(cp);
	if (target->is_point_lookup && target_list->count == 1 &&
	    self->on_match_point      &&
	    self->expr_limit  == NULL &&
	    self->expr_offset == NULL)
	{
		self->on_match_point(cp, self->on_match_arg);
		return;
	}

	// save table name
	int table_name_offset;
	table_name_offset = code_data_add_string(&cp->code_data, &target->table->config->name);

	// todo: index name
	int index_name_offset;
	Str index_name;
	str_init(&index_name);
	str_set_cstr(&index_name, "primary");
	index_name_offset = code_data_add_string(&cp->code_data, &index_name);

	// cursor_open
	int _open = op_pos(cp);
	op4(cp, CCURSOR_OPEN, target->id, table_name_offset, index_name_offset, 0 /* _where */);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof for limit expression
	if (self->climit_eof == -1)
		self->climit_eof = _where_eof;

	// _where:
	int _where = op_pos(cp);
	code_at(&cp->code, _open)->d = _where;

	// generate scan stop conditions for <, <=
	if (target->stop_ops)
		scan_generate_stop(self, target, _where_eof);

	if (target->next_join)
	{
		// recursive to inner target
		scan_generate_target(self, target->next_join);
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
			if (target->is_point_lookup)
				code_at(&cp->code, _where_jntr)->a = _where_eof;
			else
				code_at(&cp->code, _where_jntr)->a = _next;
		}
		if (self->expr_offset)
			code_at(&cp->code, _coffset_jmp)->c = _next;
	}

	// cursor_next

	// do not iterate further for point-lookups on unique index
	if (target->is_point_lookup && table_schema(target->table)->key_unique)
		op1(cp, CJMP, _where_eof);
	else
		op2(cp, CCURSOR_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(&cp->code, _where_eof)->a = _eof;
	op1(cp, CCURSOR_CLOSE, target->id);
}

static inline void
scan_generate_target_expr(Scan* self, Target* target)
{
	auto cp = self->compiler;

	// expr
	int rexpr;
	if (target->rexpr != -1)
		rexpr = target->rexpr;
	else
		rexpr = emit_expr(cp, target, target->expr);

	// cursor_open_expr
	int _open = op_pos(cp);
	op3(cp, CCURSOR_OPEN_EXPR, target->id, rexpr, 0 /* _where */);

	// _where_eof:
	int _where_eof = op_pos(cp);
	op1(cp, CJMP, 0 /* _eof */);

	// get outer target eof for limit expression
	if (self->climit_eof == -1)
		self->climit_eof = _where_eof;

	// _where:
	int _where = op_pos(cp);
	code_at(&cp->code, _open)->c = _where;

	if (target->next_join)
	{
		scan_generate_target(self, target->next_join);
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
			code_at(&cp->code, _where_jntr)->a = _next;

		if (self->expr_offset)
			code_at(&cp->code, _coffset_jmp)->c = _next;
	}

	// cursor_next
	op2(cp, CCURSOR_NEXT, target->id, _where);

	// _eof:
	int _eof = op_pos(cp);
	code_at(&cp->code, _where_eof)->a = _eof;
	op1(cp, CCURSOR_CLOSE, target->id);
	runpin(cp, rexpr);
}

static inline void
scan_generate_target(Scan* self, Target* target)
{
	if (target->table)
		scan_generate_target_table(self, target);
	else
		scan_generate_target_expr(self, target);
}

static inline void
scan_generate(Scan* self)
{
	auto cp = self->compiler;

	// offset
	if (self->expr_offset)
	{
		int roffset;
		roffset = emit_expr(cp, self->target, self->expr_offset);
		self->coffset = op_pos(cp);
		op3(cp, CCNTR_INIT, self->target->id, 1, roffset);
		runpin(cp, roffset);
	}

	// limit
	if (self->expr_limit)
	{
		int rlimit;
		rlimit = emit_expr(cp, self->target, self->expr_limit);
		self->climit = op_pos(cp);
		op3(cp, CCNTR_INIT, self->target->id, 0, rlimit);
		runpin(cp, rlimit);
	}

	scan_generate_target(self, self->target);
}

void
scan(Compiler*    compiler,
     Target*      target,
     Ast*         expr_limit,
     Ast*         expr_offset,
     Ast*         expr_where,
     ScanFunction on_match_point,
     ScanFunction on_match,
     void*        on_match_arg)
{
	Scan self =
	{
		.target         = target,
		.expr_limit     = expr_limit,
		.expr_offset    = expr_offset,
		.expr_where     = expr_where,
		.coffset        = -1,
		.climit         = -1,
		.climit_eof     = -1,
		.on_match_point = on_match_point,
		.on_match       = on_match,
		.on_match_arg   = on_match_arg,
		.compiler       = compiler
	};
	ast_list_init(&self.ops);

	// get a list of key operations from WHERE expression
	semantic_get_op(self.expr_where, &self.ops);

	// generate scan
	scan_generate(&self);
}
