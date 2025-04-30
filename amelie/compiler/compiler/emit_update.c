
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
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

hot void
emit_update_target(Compiler* self, Targets* targets, Ast* expr)
{
	auto target = targets_outer(targets);
	Ast* list = NULL;
	int  list_count = 0;

	auto columns = table_columns(target->from_table);
	auto op = expr;
	while (op)
	{
		auto next = op->next;
		op->next = NULL;

		auto name = &op->l->string;
		auto column = columns_find(columns, name);
		if (! unlikely(column))
			stmt_error(self->current, op->l,
			           "column %.*s.%.*s not found",
			           str_size(&target->name), str_of(&target->name),
			           str_size(name), str_of(name));

		if (unlikely(column->refs > 0))
			stmt_error(self->current, op->l,
			           "column %.*s.%.*s used as a part of a key",
			           str_size(&target->name), str_of(&target->name),
			           str_size(name), str_of(name));

		op->l->column = column;

		// recreate ordered list by column order
		Ast* prev = NULL;
		Ast* pos = list;
		while (pos)
		{
			if (column == pos->l->column)
				stmt_error(self->current, pos->l,
				           "column %.*s.%.*s is redefined in UPDATE",
				           str_size(&target->name), str_of(&target->name),
				           str_size(&column->name), str_of(&column->name));

			if (column->order < pos->l->column->order)
				break;

			prev = pos;
			pos = pos->next;
		}

		// insert or append
		if (prev == NULL)
		{
			op->next = list;
			list = op;
		} else
		{
			op->next = prev->next;
			prev->next = op;
		}
		list_count++;

		op = next;
	}

	// push pairs of column order and update expression
	for (op = list; op; op = op->next)
	{
		auto column = op->l->column;

		// push column order
		int rexpr = op2(self, CINT, rpin(self, TYPE_INT), column->order);
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// push expr
		int type;
		if (op->r->id == KDEFAULT)
		{
			// SET column = DEFAULT
			int offset = code_data_offset(self->code_data);
			buf_write_buf(&self->code_data->data, &column->constraints.value);
			rexpr = op2(self, CJSON, rpin(self, column->type), offset);
			type  = column->type;
		} else
		{
			// SET column = expr
			rexpr = emit_expr(self, targets, op->r);
			type  = rtype(self, rexpr);
		}
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);

		// ensure that the expression type is compatible
		// with the column
		if (unlikely(type != TYPE_NULL && column->type != type))
			stmt_error(self->current, op->l,
			           "column %.*s.%.*s update expression type '%s' does not match column type '%s'",
			           str_size(&target->name), str_of(&target->name),
			           str_size(&column->name), str_of(&column->name),
			           type_of(type),
			           type_of(column->type));
	}

	// UPDATE
	op2(self, CUPDATE, target->id, list_count);
}

static inline void
emit_update_on_match(Compiler* self, Targets* targets, void* arg)
{
	AstUpdate* update = arg;
	emit_update_target(self, targets, update->expr_update);
}

static inline void
emit_update_on_match_returning(Compiler* self, Targets* targets, void* arg)
{
	AstUpdate* update = arg;

	// update by cursor
	emit_update_target(self, targets, update->expr_update);

	// push expr and set column type
	for (auto as = update->ret.list; as; as = as->next)
	{
		auto column = as->r->column;

		// expr
		int rexpr = emit_expr(self, targets, as->l);
		int rt = rtype(self, rexpr);
		column_set_type(column, rt, type_sizeof(rt));
		op1(self, CPUSH, rexpr);
		runpin(self, rexpr);
	}

	// add to the returning set
	op1(self, CSET_ADD, update->rset);
}

hot void
emit_update(Compiler* self, Ast* ast)
{
	// UPDATE name SET column = expr [, ... ] [WHERE expr]

	// update by cursor
	auto update = ast_update_of(ast);
	if (! returning_has(&update->ret))
	{
		scan(self, &update->targets,
		     NULL,
		     NULL,
		     update->expr_where,
		     emit_update_on_match,
		     update);
		return;
	}

	// RETURNING expr

	// create returning set
	update->rset =
		op3(self, CSET, rpin(self, TYPE_STORE),
		    update->ret.count, 0);

	scan(self, &update->targets,
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
