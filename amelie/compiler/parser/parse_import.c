
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
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

hot static inline int
parse_import_value(Stmt*   self, Endpoint* endpoint,
                   Column* column,
                   Value*  column_value)
{
	// todo: support csv null columns
	unused(endpoint);

	// parse column value
	auto size = parse_value(self->lex, self->local, self->json,
	                        column,
	                        column_value);
	return size;
}

hot static inline void
parse_import_row(Stmt* self, Endpoint* endpoint)
{
	auto stmt    = ast_insert_of(self->ast);
	auto table   = endpoint->table;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

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
				// parse column value
				row_meta->row_size +=
					parse_import_value(self, endpoint, column, column_value);

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
			// parse column value
			row_meta->row_size +=
				parse_import_value(self, endpoint, column, column_value);

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
			error("incorrect number of columns in the row");
	}
}

hot static inline void
parse_import_csv(Stmt* self, Endpoint* endpoint)
{
	// ensure column list is not empty for CSV import
	if (endpoint->columns_has &&
	    endpoint->columns_count == 0)
		error("CSV import with empty column list is not supported");

	// value, ...
	for (;;)
	{
		parse_import_row(self, endpoint);
		if (stmt_if(self, KEOF))
			break;
	}
}

hot static inline void
parse_import_jsonl(Stmt* self, Endpoint* endpoint)
{
	// [value, ...]
	for (;;)
	{
		if (unlikely(! stmt_if(self, '[')))
			error("[ expected");

		parse_import_row(self, endpoint);

		if (unlikely(! stmt_if(self, ']')))
			error("incorrect number of columns in the row");

		if (stmt_if(self, KEOF))
			break;
	}
}

hot static inline void
parse_import_json(Stmt* self, Endpoint* endpoint)
{
	// [[row], ...]
	if (! stmt_if(self, '['))
		error("[ expected");

	for (;;)
	{
		// [value, ...]
		if (unlikely(! stmt_if(self, '[')))
			error("[ expected");

		parse_import_row(self, endpoint);

		if (unlikely(! stmt_if(self, ']')))
			error("incorrect number of columns in the row");

		// ,]
		if (stmt_if(self, ','))
			continue;

		if (unlikely(! stmt_if(self, ']')))
			error("] expected");

		break;
	}
}

hot void
parse_import(Parser* self, Str* str, Str* uri, EndpointType type)
{
	// prepare parser
	lex_start(&self->lex, str);

	// parse uri and get the argument list
	uri_set(&self->uri, uri, true);

	// parse endpoint path and arguments
	Endpoint endpoint;
	endpoint_init(&endpoint, &self->uri, type);
	parse_endpoint(&endpoint, self->db);

	// eof
	if (unlikely(lex_if(&self->lex, KEOF)))
		return;

	// prepare insert stmt
	auto stmt = stmt_allocate(self->db, self->function_mgr,
	                          self->local,
	                          &self->lex,
	                           self->data,
	                           self->values_cache,
	                          &self->json,
	                          &self->stmt_list,
	                          &self->cte_list,
	                           self->args);
	self->stmt = stmt;
	stmt_list_add(&self->stmt_list, stmt);
	stmt->id  = STMT_INSERT;
	stmt->ret = true;
	stmt->cte = cte_list_add(&self->cte_list, NULL, self->stmt->order);
	stmt->ast = &ast_insert_allocate()->ast;

	// create insert target
	auto level   = target_list_next_level(&stmt->target_list);
	auto table   = endpoint.table;
	auto columns = table_columns(table);
	auto target  = target_allocate();
	if (table->config->shared)
		target->type = TARGET_TABLE_SHARED;
	else
		target->type = TARGET_TABLE;
	target->from_table   = table;
	target->from_columns = columns;
	str_set_str(&target->name, &table->config->name);
	target_list_add(&stmt->target_list, target, level, 0);

	auto insert = ast_insert_of(stmt->ast);
	insert->target = target;

	// prepare result set
	insert->values = set_cache_create(stmt->values_cache);
	set_prepare(insert->values, columns->list_count, 0, NULL);

	// parse rows according to the content type
	switch (endpoint.type) {
	case ENDPOINT_CSV:
		parse_import_csv(stmt, &endpoint);
		break;
	case ENDPOINT_JSONL:
		parse_import_jsonl(stmt, &endpoint);
		break;
	case ENDPOINT_JSON:
		parse_import_json(stmt, &endpoint);
		break;
	}

	// ensure there are no data left
	if (! stmt_if(stmt, KEOF))
		error("eof expected");

	// create a list of generated columns expressions
	if (columns->generated_columns)
		parse_generated(stmt);
	
	// if table is aggregated, handle insert as upsert for
	// aggregated columns
	if (table->config->aggregated)
		parse_aggregated(stmt);
}
