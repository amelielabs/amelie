
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

hot void
parse_insert_csv(Stmt* self, Endpoint* endpoint)
{
	auto stmt = ast_insert_allocate();
	self->ast = &stmt->ast;

	// prepare insert target
	auto level  = target_list_next_level(&self->target_list);
	auto table  = endpoint->table;
	auto target = target_allocate();
	if (table->config->shared)
		target->type = TARGET_TABLE_SHARED;
	else
		target->type = TARGET_TABLE;
	target->from_table   = table;
	target->from_columns = &table->config->columns;
	str_set_str(&target->name, &table->config->name);
	target_list_add(&self->target_list, target, level, 0);
	stmt->target = target;

	// prepare values
	auto columns = table_columns(table);
	auto keys    = table_keys(table);
	stmt->values = set_cache_create(self->values_cache);
	set_prepare(stmt->values, columns->list_count, 0, NULL);

	// value[, ...] CRLF | EOF
	for (;;)
	{
		// prepare row
		SetMeta* row_meta;
		auto row = set_reserve(stmt->values, &row_meta);

		// set next serial value
		uint64_t serial = serial_next(&table->serial);

		auto list = endpoint->columns;
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			auto column_value = &row[column->order];
			auto column_separator = true;

			if (endpoint->columns_has)
			{
				if (list && list->column == column)
				{
					// todo: set null if , or EOL

					// parse column value
					row_meta->row_size +=
						parse_value(self->lex, self->local, self->json,
						            column,
						            column_value);

					list = list->next;
				} else
				{
					// default value, write SERIAL, RANDOM or DEFAULT
					row_meta->row_size +=
						parse_value_default(column, column_value, serial);
				}

				column_separator = list != NULL;
			} else
			{
				// todo: set null if , or EOL

				// parse column value
				row_meta->row_size +=
					parse_value(self->lex, self->local, self->json,
					            column,
					            column_value);

				column_separator = !list_is_last(&columns->list, &column->link);
			}

			// ensure NOT NULL constraint
			if (column_value->type == TYPE_NULL && column->constraint.not_null)
				error("column <%.*s> value cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
			
			// hash key
			if (column->key && keys_find_column(keys, column->order))
				row_meta->hash = value_hash(column_value, row_meta->hash);

			// ,
			if (column_separator && !stmt_if(self, ','))
				error("incorrect number of columns in the CSV row");
		}

		if (unlikely(stmt_if(self, ',')))
			error("incorrect number of columns in the CSV row");

		if (stmt_if(self, KEOF))
			break;
	}

	// create a list of generated columns expressions
	if (columns->generated_columns)
		parse_generated(self);
	
	// if table is aggregated, handle insert as upsert for aggregated columns
	if (table->config->aggregated)
		parse_aggregated(self);
}
