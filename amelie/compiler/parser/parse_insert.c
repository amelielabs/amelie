
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
	// (
	if (! stmt_if(self, '('))
		error("expected '('");

	// prepare row
	SetMeta* row_meta;
	auto row = set_reserve(stmt->values, &row_meta);

	// set next serial value
	uint64_t serial = serial_next(&stmt->target->from_table->serial);

	// value, ...
	auto columns = table_columns(stmt->target->from_table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		if (list && list->column->order == column->order)
		{
			// parse column value
			row_meta->row_size +=
				parse_value(self->lex, self->local, self->json,
				            column,
				            column_value);

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
				value_set_int(column_value, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				value_set_int(column_value, value);
			} else
			{
				value_decode(column_value, cons->value.start, NULL);
			}

			if (column_value->type != TYPE_NULL)
			{
				if (column_value->type == TYPE_STRING)
					row_meta->row_size += json_size_string(str_size(&column_value->string));
				else
					row_meta->row_size += column->type_size;
			}
		}

		// ensure NOT NULL constraint
		if (column_value->type == TYPE_NULL && column->constraint.not_null)
			error("column <%.*s> value cannot be NULL", str_size(&column->name),
			      str_of(&column->name));

		// hash column if it is a part of the key
		if (column->key)
			row_meta->hash = value_hash(column_value, row_meta->hash);
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

	// prepare row
	SetMeta* row_meta;
	auto row = set_reserve(stmt->values, &row_meta);

	// value, ...
	auto columns = table_columns(stmt->target->from_table);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];

		// parse column value
		row_meta->row_size +=
			parse_value(self->lex, self->local, self->json,
			            column,
			            column_value);

		// ensure NOT NULL constraint
		if (column_value->type == TYPE_NULL && column->constraint.not_null)
			error("column <%.*s> value cannot be NULL", str_size(&column->name),
			      str_of(&column->name));

		// hash column if it is a part of the key
		if (column->key)
			row_meta->hash = value_hash(column_value, row_meta->hash);

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

	auto table = stmt->target->from_table;

	auto columns = table_columns(table);
	for (auto i = 0; i < count->integer; i++)
	{
		// prepare row
		SetMeta* row_meta;
		auto row = set_reserve(stmt->values, &row_meta);

		// set next serial value
		uint64_t serial = serial_next(&table->serial);

		// value, ...
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto column_value = &row[column->order];

			// SERIAL, RANDOM or DEFAULT
			auto cons = &column->constraint;
			if (cons->serial)
			{
				value_set_int(column_value, serial);
			} else
			if (cons->random)
			{
				auto value = random_generate(global()->random) % cons->random_modulo;
				value_set_int(column_value, value);
			} else {
				value_decode(column_value, cons->value.start, NULL);
			}

			if (column_value->type != TYPE_NULL)
			{
				if (column_value->type == TYPE_STRING)
					row_meta->row_size += json_size_string(str_size(&column_value->string));
				else
					row_meta->row_size += column->type_size;
			}

			// ensure NOT NULL constraint
			if (column_value->type == TYPE_NULL && column->constraint.not_null)
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));

			// hash column if it is a part of the key
			if (column->key)
				row_meta->hash = value_hash(column_value, row_meta->hash);
		}
	}
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

hot static void
parse_generated(Stmt* self, AstInsert* stmt)
{
	auto columns = table_columns(stmt->target->from_table);

	// create a target to iterate inserted rows to create new rows
	// using the generated columns
	auto target = target_allocate();
	target->type         = TARGET_INSERTED;
	target->from_columns = stmt->target->from_columns;
	target_list_add(&self->target_list, target, stmt->target->level, 0);
	stmt->target_generated = target;

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
		Expr ctx =
		{
			.aggs   = NULL,
			.select = false
		};

		// op(column, expr)
		auto op = ast(KSET);
		op->l = ast(0);
		op->l->column = column;
		op->r = parse_expr(self, &ctx);

		if (! stmt->generated_columns)
			stmt->generated_columns = op;
		else
			tail->next = op;
		tail = op;
	}

	self->lex = lex_origin;
}

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
	auto level = target_list_next_level(&self->target_list);
	stmt->target = parse_from(self, level);
	if (stmt->target->from_table == NULL || stmt->target->next_join)
		error("INSERT INTO <table> expected");
	auto columns = stmt->target->from_columns;

	// prepare values
	stmt->values = set_cache_create(self->values_cache);
	set_prepare(stmt->values, columns->list_count, 0, NULL);

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
			if (! stmt_if(self, ','))
				break;
		}
	}

	// create a list of generated columns expressions
	if (columns->generated_columns)
		parse_generated(self, stmt);

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
