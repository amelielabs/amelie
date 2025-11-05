
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
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

static inline void
emit_insert_store_generated_on_match(Scan* self)
{
	auto       cp     = self->compiler;
	AstInsert* insert = self->on_match_arg;
	auto       target = from_first(&insert->from_generated);

	// generate and push to the stack each generated column expression
	auto count = 0;
	auto op = insert->generated_columns;
	for (; op; op = op->next)
	{
		auto column = op->l->column;

		// push column order
		int rexpr = op2pin(cp, CINT, TYPE_INT, column->order);
		op1(cp, CPUSH, rexpr);
		runpin(cp, rexpr);

		// push expr
		auto type = emit_push(cp, self->from, op->r);

		// ensure that the expression type is compatible
		// with the column
		if (unlikely(type != TYPE_NULL && column->type != type))
			stmt_error(cp->current, &insert->ast,
			           "column '%.*s.%.*s' generated expression type '%s' does not match column type '%s'",
			           str_size(&target->name), str_of(&target->name),
			           str_size(&column->name), str_of(&column->name),
			           type_of(type),
			           type_of(column->type));
		count++;
	}

	// CUPDATE_STORE
	op2(cp, CUPDATE_STORE, target->rcursor, count);
}

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
				stmt_error(stmt, row->ast, "'%s' expected for column '%.*s'",
				           type_of(column->type),
				           str_size(&column->name), str_of(&column->name));
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

	// scan over insert values to generate and apply stored columns
	if (columns->count_stored > 0)
	{
		// store_open( rvalues )
		auto values_dup = op2pin(self, CDUP, TYPE_STORE, r);
		from_first(&insert->from_generated)->r = values_dup;
		scan(self, &insert->from_generated,
		     NULL,
		     NULL,
		     NULL,
		     emit_insert_store_generated_on_match,
		     insert);
		runpin(self, values_dup);
	}

	return r;
}

hot void
emit_insert(Compiler* self, Ast* ast)
{
	// CINSERT
	auto insert = ast_insert_of(ast);
	auto target = from_first(&insert->from);
	auto table  = target->from_table;
	// set target origin
	target_set_origin(target, self->origin);
	op1(self, CINSERT, (intptr_t)table);
}
