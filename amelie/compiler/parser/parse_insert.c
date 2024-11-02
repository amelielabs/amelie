
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

hot static inline void
parse_value(Stmt* self, Column* column)
{
	// get the begining of current text position
	auto lex = self->lex;
	auto pos = lex->pos;
	while (lex->backlog)
	{
		pos = lex->backlog->pos;
		lex->backlog = lex->backlog->prev;
	}

	// parse and encode json value
	json_reset(self->json);
	Str in;
	str_set(&in, pos, self->lex->end - pos);
	json_set_time(self->json, self->local->timezone, self->local->time_us);

	// try to convert the value to the column type if possible
	int      agg  = -1;
	JsonHint hint = JSON_HINT_NONE;
	switch (column->type) {
	case TYPE_TIMESTAMP:
		hint = JSON_HINT_TIMESTAMP;
		break;
	case TYPE_INTERVAL:
		hint = JSON_HINT_INTERVAL;
		break;
	case TYPE_VECTOR:
		hint = JSON_HINT_VECTOR;
		break;
	case TYPE_AGG:
		agg  = column->constraint.aggregate;
		hint = JSON_HINT_AGG;
		break;
	}
	json_parse_hint(self->json, &in, &self->data->data, hint, agg);
	self->lex->pos = self->json->pos;
}

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
			if (cons->generated == GENERATED_SERIAL)
			{
				encode_integer(data, serial);
			} else
			if (cons->generated == GENERATED_RANDOM)
			{
				auto value = random_generate(global()->random) % cons->modulo;
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
			if (cons->generated == GENERATED_SERIAL)
			{
				encode_integer(data, serial);
			} else
			if (cons->generated == GENERATED_RANDOM)
			{
				auto value = random_generate(global()->random) % cons->modulo;
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
}

hot static inline void
parse_on_conflict(Stmt* self, AstInsert* stmt)
{
	// ON CONFLICT
	if (! stmt_if(self, KON))
		return;

	// CONFLICT
	if (! stmt_if(self, KCONFLICT))
		error("INSERT VALUES ON <CONFLICT> expected");

	// DO
	if (! stmt_if(self, KDO))
		error("INSERT VALUES ON CONFLICT <DO> expected");

	// NOTHING | UPDATE
	auto op = stmt_next(self);
	switch (op->id) {
	case KNOTHING:
		stmt->on_conflict = ON_CONFLICT_NOTHING;
		break;
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
	default:
		error("INSERT VALUES ON CONFLICT DO <NOTHING | UPDATE> expected");
		break;
	}
}

hot void
parse_insert(Stmt* self)
{
	// INSERT INTO name [(column_list)]
	// [GENERATE | VALUES] (value, ..), ...
	// [ON CONFLICT DO NOTHING | UPDATE]
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
