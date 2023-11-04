
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
#include <monotone_compiler.h>

static inline void
emit_update_on_match(Compiler* self, void *arg)
{
	AstUpdate* update = arg;

	// start from current object
	int r = op2(self, CCURSOR_READ, rpin(self), update->target->id);
	op1(self, CPUSH, r);
	runpin(self, r);

	auto op = update->expr_update;
	for (; op; op = op->next)
	{
		auto path = op->l;

		// update column in a row
		auto def = table_def(update->target->table);

		Column* column = NULL;
		switch (path->id) {
		case KNAME:
		{
			column = def_find_column(def, &path->string);
			if (! column)
				error("<%.*s> column does not exists", str_size(&path->string),
				      str_of(&path->string));
			path->id = KNULL;
			break;
		}
		case KNAME_COMPOUND:
		{
			Str name;
			str_split_or_set(&path->string, &name, '.');

			column = def_find_column(def, &name);
			if (! column)
				error("<%.*s> column does not exists", str_size(&name),
				      str_of(&name));

			// exclude name from the path
			str_advance(&path->string, str_size(&name) + 1);
			path->id = KSTRING;
			break;
		}
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
				if (path->id == KNULL || str_compare_prefix(&key->path, &path->string))
					error("<%.*s> column nested key <%.*s> cannot be updated",
					      str_size(&column->name), str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));
			}
		}

		// todo: check secondary keys

		// path
		int rexpr = emit_expr(self, update->target, op->l);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// expr
		rexpr = emit_expr(self, update->target, op->r);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// assign(obj, idx, expr, column)
		rexpr = op3(self, CASSIGN, rpin(self), 3, column->order);

		// push
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}

	// UPDATE (last modified object)
	op1(self, CUPDATE, update->target->id);
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
