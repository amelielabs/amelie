
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

hot static void
parse_row_list(Stmt* self, Table* table, Set* values, Ast* list)
{
	// (
	stmt_expect(self, '(');

	// prepare row
	auto row = set_reserve(values);

	// set next sequence value for identity columns
	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (!value && list && list->column->order == column->order)
		{
			// parse column value
			value = parse_value(self, column, column_value);

			// ,
			list = list->next;
			if (list)
				stmt_expect(self, ',');

		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate(self, column, column_value, value);
	}

	// )
	stmt_expect(self, ')');
}

hot static void
parse_row(Stmt* self, Table* table, Set* values)
{
	// (
	stmt_expect(self, '(');

	// prepare row
	auto row = set_reserve(values);

	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (! value)
		{
			// parse column value
			value = parse_value(self, column, column_value);
		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate(self, column, column_value, value);

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				stmt_error(self, NULL, "row has incorrect number of columns");
			continue;
		}
	}

	// )
	stmt_expect(self, ')');
}

hot void
parse_row_generate(Stmt* self, Table* table, Set* values, int count)
{
	// insert into () values (), ...
	auto columns = table_columns(table);
	for (auto i = 0; i < count; i++)
	{
		// prepare row
		auto row = set_reserve(values);

		// set next sequence value for identity columns
		uint64_t seq = sequence_next(&table->seq);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto column_value = &row[column->order];

			// IDENTITY, RANDOM or DEFAULT
			parse_value_default(self, column, column_value, seq);

			// ensure NOT NULL constraint
			parse_value_validate(self, column, column_value, NULL);
		}
	}
}

void
parse_rows(Stmt* self, Table* table, Set* values,
           Ast*  list,
           bool  list_in_use)
{
	// VALUES (value[, ...])[, ...]
	for (;;)
	{
		if (list_in_use)
			parse_row_list(self, table, values, list);
		else
			parse_row(self, table, values);
		if (! stmt_if(self, ','))
			break;
	}
}

hot static Ast*
parse_row_list_expr(Stmt* self, Table* table, Ast* list)
{
	// (
	stmt_expect(self, '(');

	Ast* row = NULL;
	Ast* row_tail = NULL;

	// set next sequence value for identity columns
	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (!value && list && list->column->order == column->order)
		{
			// parse column expr
			value = parse_expr(self, NULL);

			// ,
			list = list->next;
			if (list)
				stmt_expect(self, ',');
		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			value = parse_value_default_expr(self, column, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate_expr(self, column, value);

		if (row_tail)
			row_tail->next = value;
		else
			row = value;
		row_tail = value;
	}

	// )
	stmt_expect(self, ')');
	return row;
}

hot static Ast*
parse_row_expr(Stmt* self, Table* table)
{
	// (
	stmt_expect(self, '(');

	Ast* row = NULL;
	Ast* row_tail = NULL;

	uint64_t seq = sequence_next(&table->seq);

	// value, ...
	auto columns = table_columns(table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// DEFAULT | value
		Ast* value = stmt_if(self, KDEFAULT);
		if (! value)
		{
			// parse column value
			value = parse_expr(self, NULL);
		} else
		{
			// IDENTITY, RANDOM or DEFAULT
			value = parse_value_default_expr(self, column, seq);
		}

		// ensure NOT NULL constraint
		parse_value_validate_expr(self, column, value);

		if (row_tail)
			row_tail->next = value;
		else
			row = value;
		row_tail = value;

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				stmt_error(self, NULL, "row has incorrect number of columns");
			continue;
		}
	}

	// )
	stmt_expect(self, ')');
	return row;
}

hot void
parse_row_generate_expr(Stmt* self, Table* table, AstList* rows, int count)
{
	// insert into () values (), ...
	auto columns = table_columns(table);
	for (auto i = 0; i < count; i++)
	{
		// prepare row
		Ast* row = NULL;
		Ast* row_tail = NULL;

		// set next sequence value for identity columns
		uint64_t seq = sequence_next(&table->seq);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);

			// IDENTITY, RANDOM or DEFAULT
			auto value = parse_value_default_expr(self, column, seq);

			// ensure NOT NULL constraint
			parse_value_validate_expr(self, column, value);

			if (row_tail)
				row_tail->next = value;
			else
				row = value;
			row_tail = value;
		}
		ast_list_add(rows, row);
	}
}

void
parse_rows_expr(Stmt* self, Table* table, AstList* rows,
                Ast*  list,
                bool  list_in_use)
{
	// VALUES (value[, ...])[, ...]
	for (;;)
	{
		Ast* row;
		if (list_in_use)
			row = parse_row_list_expr(self, table, list);
		else
			row = parse_row_expr(self, table);
		ast_list_add(rows, row);
		if (! stmt_if(self, ','))
			break;
	}
}
