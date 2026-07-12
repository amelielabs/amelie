
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_parser.h>
#include <amelie_plan.h>
#include <amelie_compiler.h>

static int
emit_insert_store_rows(Compiler* self)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = from_first(&insert->from)->from_table;
	auto columns = table_columns(table);

	// emit rows
	auto rset = op3pin(self, CSET, TYPE_STORE, columns->count, 0);
	for (auto row = insert->rows.list; row; row = row->next)
	{
		auto col = row->ast;
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto type = emit_push(self, &insert->from, col);
			if (unlikely(type != TYPE_NULL && column->type != type))
				stmt_error(stmt, row->ast, "'{s}' expected for column '{str}",
				           type_of(column->type),
				           &column->name);
			col = col->next;
		}
		op1(self, CSET_ADD, rset);
	}
	return op2pin(self, CDUP, TYPE_STORE, rset);
}

int
emit_insert_store(Compiler* self)
{
	auto stmt    = self->current;
	auto insert  = ast_insert_of(stmt->ast);
	auto target  = from_first(&insert->from);
	auto table   = target->from_table;
	auto columns = table_columns(table);

	// set target origin
	target_set_origin(target, self->origin);

	// create or match a set to use with the insert operation
	int r;
	if (insert->select)
	{
		// use rows set returned from select
		auto columns_select = &ast_select_of(insert->select->ast)->ret.columns;
		if (! columns_compare(columns, columns_select))
			stmt_error(stmt, insert->select->ast, "SELECT columns must match the INSERT table");

		r = op2pin(self, CDUP, TYPE_STORE, insert->select->r);
	} else
	{
		if (insert->rows.count > 0)
			// use rows as expressions provided during parsing
			r = emit_insert_store_rows(self);
		else
			// use rows set created during parsing
			r = op2pin(self, CSET_PTR, TYPE_STORE, (intptr_t)insert->values);
	}

	return r;
}

hot void
emit_insert(Compiler* self, Ast* ast)
{
	// CINSERT
	auto insert = ast_insert_of(ast);
	auto target = from_first(&insert->from);
	// set target origin
	target_set_origin(target, self->origin);
	op2(self, CINSERT, (intptr_t)target->from_table,
	    (intptr_t)target->from_timeline);
}
