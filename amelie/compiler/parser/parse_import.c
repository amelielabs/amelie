
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

hot static inline void
parse_import_row(Stmt* self, Endpoint* endpoint)
{
	auto stmt    = ast_insert_of(self->ast);
	auto table   = endpoint->table;
	auto columns = table_columns(table);

	// prepare row
	auto row = set_reserve(stmt->values);

	// set next sequence value
	uint64_t seq = sequence_next(&table->seq);

	auto list = endpoint->columns;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];
		auto column_separator = true;

		Ast* value = NULL;
		if (endpoint->columns_has)
		{
			if (list && list->column == column)
			{
				// parse column value
				value = parse_value(self, column, column_value);
				list = list->next;
				column_separator = list != NULL;
			} else
			{
				// default value, write IDENTITY, RANDOM or DEFAULT
				parse_value_default(self, column, column_value, seq);
				column_separator = false;
			}
		} else
		{
			// parse column value
			value = parse_value(self, column, column_value);
			column_separator = !list_is_last(&columns->list, &column->link);
		}

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, value);

		// ,
		if (column_separator)
			stmt_expect(self, ',');
	}
}

hot static inline void
parse_import_obj(Stmt* self, Endpoint* endpoint)
{
	auto stmt    = ast_insert_of(self->ast);
	auto table   = endpoint->table;
	auto columns = table_columns(table);

	// prepare row
	auto row = set_reserve(stmt->values);

	auto buf = buf_create();
	defer_buf(buf);
	buf_reserve(buf, sizeof(bool) * columns->count);
	memset(buf->start, 0, sizeof(bool) * columns->count);

	auto match = (bool*)buf->start;
	auto match_count = 0;
	for (;;)
	{
		// "key"
		auto key = stmt_if(self, KSTRING);
		if (! key)
			break;

		// :
		stmt_expect(self, ':');

		// match column
		auto column = columns_find(columns, &key->string);
		if (! column)
			stmt_error(self, key, "column does not exists");

		// ensure column is not redefined
		if (unlikely(match[column->order]))
			stmt_error(self, key, "column value is redefined");
		match[column->order] = true;
		match_count++;

		// parse column value
		auto column_value = &row[column->order];
		auto value = parse_value(self, column, column_value);

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}
	if (match_count == columns->count)
		return;

	// set next sequence value
	uint64_t seq = sequence_next(&table->seq);

	// default value, write IDENTITY, RANDOM or DEFAULT
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (match[column->order])
			continue;

		auto column_value = &row[column->order];
		parse_value_default(self, column, column_value, seq);

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, NULL);
	}
}

hot static inline void
parse_import_json_row(Stmt* self, Endpoint* endpoint)
{
	// {} or []
	auto begin = stmt_if(self, '{');
	if (begin)
	{
		if (endpoint->columns_has)
		{
			if (unlikely(endpoint->columns_count > 1))
				stmt_error(self, begin, "JSON column list must have zero or one value");

			// process {} as a value for column, if one column
			if (endpoint->columns_count == 1)
				stmt_push(self, begin);

			parse_import_row(self, endpoint);
		} else {
			parse_import_obj(self, endpoint);
		}
		if (! endpoint->columns_count)
			stmt_expect(self, '}');
		return;
	}

	if (! stmt_if(self, '['))
		stmt_error(self, NULL, "json object or array expected");
	parse_import_row(self, endpoint);
	stmt_expect(self, ']');
}

hot static inline void
parse_import_jsonl(Stmt* self, Endpoint* endpoint)
{
	for (;;)
	{
		// {} or []
		parse_import_json_row(self, endpoint);
		if (stmt_if(self, KEOF))
			break;
	}
}

hot static inline void
parse_import_json(Stmt* self, Endpoint* endpoint)
{
	// [ {} or [], ...]
	stmt_expect(self, '[');
	for (;;)
	{
		// {} or []
		parse_import_json_row(self, endpoint);
		// ,]
		if (stmt_if(self, ','))
			continue;
		stmt_expect(self, ']');
		break;
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

	auto scope = scopes_add(&self->scopes, NULL, NULL);

	// prepare insert stmt
	auto stmt = stmt_allocate(self, &self->lex, scope);
	self->stmt = stmt;
	stmts_add(&self->stmts, stmt);
	stmt->id  = STMT_INSERT;
	stmt->ret = true;
	stmt->ast = &ast_insert_allocate(scope)->ast;

	// create insert target
	auto insert  = ast_insert_of(stmt->ast);
	auto table   = endpoint.table;
	auto columns = table_columns(table);
	auto target  = target_allocate(&stmt->order_targets);
	target->type         = TARGET_TABLE;
	target->ast          = stmt->ast;
	target->from_access  = ACCESS_RW;
	target->from_table   = table;
	target->from_columns = columns;
	str_set_str(&target->name, &table->config->name);
	targets_add(&insert->targets, target);
	access_add(&self->program->access, &table->rel, ACCESS_RW);

	// prepare result set
	insert->values = set_cache_create(stmt->parser->values_cache);
	set_prepare(insert->values, columns->count, 0, NULL);

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
		stmt_error(stmt, NULL, "eof expected");

	// create a list of generated columns expressions
	if (columns->count_stored > 0)
		parse_generated(stmt);
	
	// if table has resolved columns, handle insert as upsert
	// and apply the resolve expressions on conflicts
	if (columns->count_resolved > 0)
		parse_resolved(stmt);
}
