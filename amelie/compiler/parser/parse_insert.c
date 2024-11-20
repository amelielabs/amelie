
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot static void
parse_row_list(Stmt* self, AstInsert* stmt, Ast* list)
{
	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// begin new row
	auto columns = table_columns(stmt->target->from_table);
	auto row_data = self->data_row;
	row_data_begin(row_data, columns->list_count);

	// set next serial value
	uint64_t serial = serial_next(&stmt->target->from_table->serial);

	// value, ...
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_data = row_data_add(row_data);
		auto is_null = false;

		if (list && list->column->order == column->order)
		{
			// parse column value
			if (stmt_if(self, KNULL))
				is_null = true;
			else
				parse_value(self->lex, self->local, self->json, column_data, column);

			// ,
			list = list->next;
			if (list) {
				if (! stmt_if(self, ','))
					error("row has incorrect number of columns");
			}

		} else
		{
			// SERIAL, RANDOM or DEFAULT
			auto cons = &column->constraint;
			if (cons->serial)
			{
				buf_write_i64(column_data, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				buf_write_i64(column_data, value);
			} else
			{
				is_null = json_is_null(cons->value.start);
				if (! is_null)
				{
					// TODO: read data based on column type
				}
			}
		}

		// set column to null
		if (is_null)
		{
			// ensure NOT NULL constraint
			if (column->constraint.not_null)
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
			row_data_set_null(row_data);
		}

		// hash column if it is a part of the key
		if (column->key)
			row_data_hash(row_data);
	}

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline void
parse_row(Stmt* self, AstInsert* stmt)
{
	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// begin new row
	auto columns = table_columns(stmt->target->from_table);
	auto row_data = self->data_row;
	row_data_begin(row_data, columns->list_count);

	// value, ...
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_data = row_data_add(row_data);
		auto is_null = false;

		// parse column value
		if (stmt_if(self, KNULL))
			is_null = true;
		else
			parse_value(self->lex, self->local, self->json, column_data, column);

		// set column to null
		if (is_null)
		{
			// ensure NOT NULL constraint
			if (column->constraint.not_null)
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
			row_data_set_null(row_data);
		}

		// hash column if it is a part of the key
		if (column->key)
			row_data_hash(row_data);

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				error("row has incorrect number of columns");
			continue;
		}
	}

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline Ast*
parse_column_list(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->from_table);

	// empty column list ()
	if (unlikely(stmt_if(self, ')')))
		return NULL;

	Ast* list = NULL;
	Ast* list_last = NULL;
	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (unlikely(! name))
			error("INSERT INTO name (<column name> expected");

		// find column and validate order
		auto column = columns_find(columns, &name->string);
		if (! column)
			error("<%.*s> column does not exists", str_size(&name->string),
			      str_of(&name->string));

		if (list == NULL) {
			list = name;
		} else
		{
			// validate column order
			if (column->order <= list_last->column->order)
				error("column list must be in order");

			list_last->next = name;
		}
		list_last = name;

		// set column
		name->column = column;

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (! stmt_if(self, ')'))
			error("expected ')'");

		break;
	}

	return list;
}

hot static inline void
parse_generate(Stmt* self, AstInsert* stmt)
{
	// insert into () values (), ...

	// GENERATE count
	auto count = stmt_if(self, KINT);
	if (! count)
		error("GENERATE <count> expected");

	auto row_data = self->data_row;
	auto table = stmt->target->from_table;
	auto columns = table_columns(table);
	for (auto i = 0; i < count->integer; i++)
	{
		// begin new row
		row_data_begin(row_data, columns->list_count);

		// set next serial value
		uint64_t serial = serial_next(&table->serial);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto column_data = row_data_add(row_data);
			auto is_null = false;

			// SERIAL, RANDOM or DEFAULT
			auto cons = &column->constraint;
			if (cons->serial)
			{
				buf_write_i64(column_data, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				buf_write_i64(column_data, value);
			} else
			{
				is_null = json_is_null(cons->value.start);
				if (! is_null)
				{
					// TODO: read data based on column type
				}
			}

			// set column to null
			if (is_null)
			{
				// ensure NOT NULL constraint
				if (column->constraint.not_null)
					error("column <%.*s> value cannot be NULL", str_size(&column->name),
					      str_of(&column->name));
				row_data_set_null(row_data);
			}

			// hash column if it is a part of the key
			if (column->key)
				row_data_hash(row_data);
		}
	}

	stmt->rows_count = count->integer;
}

hot static inline void
parse_on_conflict_aggregate(Stmt* self, AstInsert* stmt)
{
	stmt->on_conflict = ON_CONFLICT_UPDATE;
	// handle insert as upsert and generate
	//
	// SET <column> = <aggregated column expression> [, ...]
	auto columns = table_columns(stmt->target->from_table);
	stmt->update_expr = parse_update_aggregated(self, columns);
	if (! stmt->update_expr)
		stmt->on_conflict = ON_CONFLICT_NOTHING;
}

hot static inline void
parse_on_conflict(Stmt* self, AstInsert* stmt)
{
	// ON CONFLICT
	if (! stmt_if(self, KON))
	{
		// if table is aggregated and no explicit ON CONFLICT clause
		// then handle as ON CONFLICT DO AGGREGATE
		if (stmt->target->from_table->config->aggregated)
			parse_on_conflict_aggregate(self, stmt);
		return;
	}

	// CONFLICT
	if (! stmt_if(self, KCONFLICT))
		error("INSERT VALUES ON <CONFLICT> expected");

	// DO
	if (! stmt_if(self, KDO))
		error("INSERT VALUES ON CONFLICT <DO> expected");

	// NOTHING | UPDATE | AGGREGATE
	auto op = stmt_next(self);
	switch (op->id) {
	case KNOTHING:
	{
		stmt->on_conflict = ON_CONFLICT_NOTHING;
		break;
	}
	case KUPDATE:
	{
		stmt->on_conflict = ON_CONFLICT_UPDATE;

		// SET path = expr [, ... ]
		stmt->update_expr = parse_update_expr(self);

		// [WHERE]
		if (stmt_if(self, KWHERE))
			stmt->update_where = parse_expr(self, NULL);
		break;
	}
	case KAGGREGATE:
	{
		// handle insert as upsert and generate
		//
		// SET <column> = <aggregated column expression> [, ...]
		parse_on_conflict_aggregate(self, stmt);
		break;
	}
	default:
		error("INSERT VALUES ON CONFLICT DO <NOTHING | UPDATE | AGGREGATE> expected");
		break;
	}
}

#if 0
hot static void
parse_generated(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->from_table);
	if (! columns->generated_columns)
		return;

	// create a target to iterate inserted rows to create new rows
	// using the generated columns
	stmt->target_generated =
			target_list_add(&self->target_list, stmt->target->level, 0,
			                NULL, NULL, columns, NULL, NULL);

	// parse generated columns expressions
	auto lex_origin = self->lex;
	Lex lex;
	lex_init(&lex, keywords);
	self->lex = &lex;

	Ast* tail = NULL;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (str_empty(&column->constraint.as_stored))
			continue;
		lex_init(&lex, keywords);
		lex_start(&lex, &column->constraint.as_stored);
		Expr expr =
		{
			.aggs   = NULL,
			.select = false
		};
		auto ast = parse_expr(self, &expr);
		if (! stmt->generated_columns)
			stmt->generated_columns = ast;
		else
			tail->next = ast;
		tail = ast;
	}

	self->lex = lex_origin;
}
#endif

hot void
parse_insert(Stmt* self)
{
	// INSERT INTO name [(column_list)]
	// [GENERATE | VALUES] (value, ..), ...
	// [ON CONFLICT DO NOTHING | UPDATE | AGGREGATE]
	// [RETURNING expr]
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// INTO
	if (! stmt_if(self, KINTO))
		error("INSERT <INTO> expected");

	// table
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	if (stmt->target->from_table == NULL || stmt->target->next_join)
		error("INSERT INTO <table> expected");

	// set offset to the first row
	stmt->rows = self->data_row->rows;

	// GENERATE
	if (stmt_if(self, KGENERATE))
	{
		parse_generate(self, stmt);
	} else
	{
		// (column list)
		Ast* list = NULL;
		bool list_in_use = stmt_if(self, '(');
		if (list_in_use)
			list = parse_column_list(self, stmt);

		// VALUES (value[, ...])[, ...]
		if (! stmt_if(self, KVALUES))
			error("INSERT INTO <VALUES> expected");
		for (;;)
		{
			if (list_in_use)
				parse_row_list(self, stmt, list);
			else
				parse_row(self, stmt);
			stmt->rows_count++;
			if (! stmt_if(self, ','))
				break;
		}
	}

	// TODO
	//  create a list of generated columns expressions
	//parse_generated(self, stmt);

	// ON CONFLICT
	parse_on_conflict(self, stmt);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		parse_returning(&stmt->ret, self, NULL);
		parse_returning_resolve(&stmt->ret, self, stmt->target);

		// convert insert to upsert ON CONFLICT ERROR to support returning
		if (stmt->on_conflict == ON_CONFLICT_NONE)
			stmt->on_conflict = ON_CONFLICT_ERROR;
	}
}
