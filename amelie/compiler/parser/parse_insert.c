
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

hot static void
parse_row_list(Stmt* self, AstInsert* stmt, Ast* list)
{
	auto columns = table_columns(stmt->target->table);

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// set next serial value
	uint64_t serial = serial_next(&stmt->target->table->serial);

	// [
	auto data = &self->data->data;
	encode_array(data);

	// value, ...
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto offset = code_data_offset(self->data);
		if (list && list->column->order == column->order)
		{
			// parse and encode json value
			parse_value(self, column);

			// ,
			list = list->next;
			if (list) {
				if (! stmt_if(self, ','))
					error("row has incorrect number of columns");
			}

		} else
		{
			// GENERATED
			auto cons = &column->constraint;
			if (cons->serial)
			{
				encode_integer(data, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				encode_integer(data, value);
			} else
			{
				// use DEFAULT (NULL by default)
				buf_write(data, cons->value.start, buf_size(&cons->value));
			}
		}

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			if (data_is_null(code_data_at(self->data, offset)))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
		}
	}

	// ]
	encode_array_end(data);

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline void
parse_row(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->table);

	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// [
	auto data = &self->data->data;
	encode_array(data);

	// value, ...
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// parse and encode json value
		auto offset = code_data_offset(self->data);
		parse_value(self, column);

		// ensure NOT NULL constraint
		if (column->constraint.not_null)
		{
			if (data_is_null(code_data_at(self->data, offset)))
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
		}

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				error("row has incorrect number of columns");
			continue;
		}
	}

	// ]
	encode_array_end(data);

	// )
	if (! stmt_if(self, ')'))
		error("expected ')'");
}

hot static inline void
parse_values(Stmt* self, AstInsert* stmt, bool list_in_use, Ast* list)
{
	// [VALUES]
	if (stmt_if(self, KVALUES))
	{
		// (value, ...), ...
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
		return;
	}

	// one column case
	auto columns = table_columns(stmt->target->table);
	if (unlikely(list || columns->list_count > 1))
		error("INSERT INTO <VALUES> expected");

	// no constraints applied
	auto data = &self->data->data;

	// value, ...
	for (;;)
	{
		// [
		encode_array(data);

		// parse and encode json value
		auto column = columns_find_by(columns, 0);
		parse_value(self, column);

		// ]
		encode_array_end(data);

		stmt->rows_count++;
		if (! stmt_if(self, ','))
			break;
	}
}

hot static inline Ast*
parse_column_list(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->table);

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

	auto data = &self->data->data;
	auto columns = table_columns(stmt->target->table);
	for (uint64_t i = 0; i < count->integer; i++)
	{
		// set next serial value
		uint64_t serial = serial_next(&stmt->target->table->serial);

		// [
		encode_array(data);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			// GENERATED
			auto cons = &column->constraint;
			if (cons->serial)
			{
				encode_integer(data, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				encode_integer(data, value);
			} else
			{
				// ensure NOT NULL constraint (for DEFAULT value)
				if (cons->not_null)
				{
					if (data_is_null(cons->value.start))
						error("column <%.*s> value cannot be NULL", str_size(&column->name),
						      str_of(&column->name));
				}

				// use DEFAULT (NULL by default)
				buf_write(data, cons->value.start, buf_size(&cons->value));
			}
		}

		// ]
		encode_array_end(data);
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
	auto columns = table_columns(stmt->target->table);
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
		if (stmt->target->table->config->aggregated)
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

hot static void
parse_generated(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->table);
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

hot void
parse_insert(Stmt* self)
{
	// INSERT INTO name [(column_list)]
	// [GENERATE | VALUES] (value, ..), ...
	// [ON CONFLICT DO NOTHING | UPDATE | AGGREGATE]
	// [RETURNING expr [INTO cte]]
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// INTO
	if (! stmt_if(self, KINTO))
		error("INSERT <INTO> expected");

	// table
	int level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	if (stmt->target->table == NULL || stmt->target->next_join)
		error("INSERT INTO <table> expected");

	// [
	stmt->rows = code_data_offset(self->data);
	auto data = &self->data->data;
	encode_array(data);

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

		// [VALUES] | expr
		parse_values(self, stmt, list_in_use, list);
	}

	// ]
	encode_array_end(data);

	//  create a list of generated columns expressions
	parse_generated(self, stmt);

	// ON CONFLICT
	parse_on_conflict(self, stmt);

	// [RETURNING]
	if (stmt_if(self, KRETURNING))
	{
		stmt->returning = parse_expr(self, NULL);
		// convert insert to upsert ON CONFLICT ERROR to support returning
		if (stmt->on_conflict == ON_CONFLICT_NONE)
			stmt->on_conflict = ON_CONFLICT_ERROR;

		// [INTO cte_name]
		if (stmt_if(self, KINTO))
			parse_cte(self, false, false);
	}
}
