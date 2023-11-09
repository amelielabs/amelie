
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>

hot void
emit_update_target(Compiler* self, Target* target, Ast* expr)
{
	// start from current object
	int r = op2(self, CCURSOR_READ, rpin(self), target->id);
	op1(self, CPUSH, r);
	runpin(self, r);

	auto op = expr;
	for (; op; op = op->next)
	{
		Str path;
		str_set_str(&path, &op->l->string);

		// update column in a row
		auto def = table_def(target->table);
		Column* column = NULL;
		switch (op->l->id) {
		case KNAME:
		{
			column = def_find_column(def, &path);
			if (! column)
				error("<%.*s> column does not exists", str_size(&path),
				      str_of(&path));
			str_init(&path);
			break;
		}
		case KNAME_COMPOUND:
		{
			Str name;
			str_split_or_set(&path, &name, '.');

			column = def_find_column(def, &name);
			if (! column)
				error("<%.*s> column does not exists", str_size(&name),
				      str_of(&name));

			// exclude name from the path
			str_advance(&path, str_size(&name) + 1);
			break;
		}
		default:
			abort();
			break;
		}

		// ensure we are not updating a key
		auto key = column->key;
		for (; key; key = key->next_column)
		{
			if (str_empty(&key->path)) {
				error("<%.*s> key columns cannot be updated", str_size(&column->name),
				      str_of(&column->name));
			} else
			{
				// path is equal or a prefix of the key path
				if (str_empty(&path) || str_compare_prefix(&key->path, &path))
					error("<%.*s> column nested key <%.*s> cannot be updated",
					      str_size(&column->name), str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));
			}
		}

		// todo: check secondary keys

		// path
		int rexpr;
		if (str_empty(&path))
			rexpr = op1(self, CNULL, rpin(self));
		else
			rexpr = emit_string(self, &path, false);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// expr
		rexpr = emit_expr(self, target, op->r);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// assign(obj, idx, expr, column)
		rexpr = op3(self, CASSIGN, rpin(self), 3, column->order);

		// push
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}

	// UPDATE (last modified object)
	op1(self, CUPDATE, target->id);
}

static inline void
emit_update_on_match(Compiler* self, void *arg)
{
	AstUpdate* update = arg;
	emit_update_target(self, update->target, update->expr_update);
}

void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name SET path = expr [, ... ] [WHERE expr]
	auto update = ast_update_of(ast);

	// update by cursor
	scan(self, update->target,
	     NULL,
	     NULL,
	     update->expr_where,
	     NULL,
	     emit_update_on_match,
	     update);
}
