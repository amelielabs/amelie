
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
#include <amelie_data.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>

hot void
emit_update_target(Compiler* self, Target* target, Ast* expr)
{
	// start from current object
	int r = op2(self, CCURSOR_READ, rpin(self), target->id);
	op1(self, CPUSH, r);
	runpin(self, r);

	auto columns = table_columns(target->table);
	auto op = expr;
	for (; op; op = op->next)
	{
		Str path;
		str_set_str(&path, &op->l->string);

		// update column in a row
		Column* column = NULL;
		switch (op->l->id) {
		case KNAME:
		{
			column = columns_find(columns, &path);
			if (! column)
				error("<%.*s> column does not exists", str_size(&path),
				      str_of(&path));
			str_init(&path);
			break;
		}
		case KNAME_COMPOUND:
		{
			Str name;
			str_split(&path, &name, '.');

			column = columns_find(columns, &name);
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

		// ensure column is not virtual
		if (unlikely(column->constraint.generated == GENERATED_VIRTUAL))
			error("virtual columns cannot be updated");

		// ensure we are not updating a key
		if (column->key)
		{
			list_foreach(&target->table->config->indexes)
			{
				auto index = list_at(IndexConfig, link);
				list_foreach(&index->keys.list)
				{
					auto key = list_at(Key, link);
					if (key->column != column)
						continue;

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
			}
		}

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
emit_update_on_match(Compiler* self, void* arg)
{
	AstUpdate* update = arg;
	emit_update_target(self, update->target, update->expr_update);
}

static inline void
emit_update_on_match_returning(Compiler* self, void* arg)
{
	AstUpdate* update = arg;
	emit_update_target(self, update->target, update->expr_update);

	// expr
	int rexpr = emit_expr(self, update->target, update->returning);

	// add to the returning set
	op2(self, CSET_ADD, update->rset, rexpr);
	runpin(self, rexpr);
}

hot void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name SET path = expr [, ... ] [WHERE expr]
	auto update = ast_update_of(ast);

	// update by cursor
	if (! update->returning)
	{
		scan(self, update->target,
		     NULL,
		     NULL,
		     update->expr_where,
		     emit_update_on_match,
		     update);
		return;
	}

	// RETURNING expr

	// create returning set
	update->rset = op1(self, CSET, rpin(self));

	scan(self, update->target,
	     NULL,
	     NULL,
	     update->expr_where,
	     emit_update_on_match_returning,
	     update);

	// CRESULT (return set)
	op1(self, CRESULT, update->rset);
	runpin(self, update->rset);
	update->rset = -1;
}
