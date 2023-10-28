
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

		int column_order = -1;
		switch (path->id) {
		case KNAME:
		{
			auto column = def_find_column(def, &path->string);
			if (! column)
				error("<%.*s> column does not exists", str_size(&path->string),
				      str_of(&path->string));
			column_order = column->order;
			path->id = KNULL;
			break;
		}
		case KNAME_COMPOUND:
		{
			Str name;
			str_split_or_set(&path->string, &name, '.');

			auto column = def_find_column(def, &name);
			if (! column)
				error("<%.*s> column does not exists", str_size(&name),
				      str_of(&name));
			column_order = column->order;

			// exclude name from the path
			str_advance(&path->string, str_size(&name) + 1);
			path->id = KSTRING;
			break;
		}
		case KINT:
		{
			column_order = path->integer;
			path->id = KNULL;
			break;
		}
		}

		// ensure we are not updating a key with the same path
		auto key_column = def_find_key_by_order(def, column_order);
		if (key_column)
			error("<%.*s> key columns cannot be updated",
			      str_size(&key_column->name),
			      str_of(&key_column->name));

		// todo: check secondary keys

		// ensure it is safe to remove a column
		if (op->id == KUNSET)
		{
			if (column_order < def->column_count)
				error("%s", "fixed columns cannot be removed");
		}

		// path
		int rexpr = emit_expr(self, update->target, op->l);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// expr
		if (op->r)
		{
			rexpr = emit_expr(self, update->target, op->r);
			op1(self, CPUSH, rexpr);
			runpin(self, rexpr);
		}

		// op
		switch (op->id) {
		case KSET:
			// col_set(obj, idx, expr, column)
			rexpr = op3(self, CCOL_SET, rpin(self), 3, column_order);
			break;
		case KUNSET:
			// col_unset(obj, idx, column)
			rexpr = op3(self, CCOL_UNSET, rpin(self), 2, column_order);
			break;
		}

		// push
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}
}

void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name [SET|UNSET] path = expr [, ... ]
	// [WHERE expr]
	// [LIMIT expr]
	// [OFFSET expr]
	auto update = ast_update_of(ast);

	// update by cursor
	scan(self, update->target,
	     update->expr_limit,
	     update->expr_offset,
	     update->expr_where,
	     NULL,
	     emit_update_on_match,
	     update);
}
