
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_planner.h>
#include <sonata_compiler.h>

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
			str_split_or_set(&path, &name, '.');

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
emit_update_on_match(Compiler* self, void *arg)
{
	AstUpdate* update = arg;
	emit_update_target(self, update->target, update->expr_update);
}

hot void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name SET path = expr [, ... ] [WHERE expr]
	auto update = ast_update_of(ast);

	// update by cursor
	scan(self, update->target,
	     NULL,
	     NULL,
	     update->expr_where,
	     emit_update_on_match,
	     update);
}
