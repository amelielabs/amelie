
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>

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

hot void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name SET path = expr [, ... ] [WHERE expr]
	auto update = ast_update_of(ast);
	auto stmt   = self->current;

	// update by cursor
	scan(self, update->target,
	     NULL,
	     NULL,
	     update->expr_where,
	     emit_update_on_match,
	     update);

	// CREADY
	op2(self, CREADY, stmt->order, -1);

	if (update->target->table->config->reference)
	{
		// get route by order
		auto route = router_at(self->router, 0);

		// get the request
		auto req = dispatch_add(self->dispatch, stmt->order, route);

		// append code
		op_relocate(&req->code, &self->code_stmt);

	} else
	{
		// copy statement code to each shard
		compiler_distribute(self);
	}

	// coordinator
	compiler_switch(self, &self->code_coordinator);

	// CRECV
	op0(self, CRECV);
}
