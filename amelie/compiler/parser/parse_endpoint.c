
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
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

#if 0
static inline bool
parse_endpoint_path(Str* self, Str* db, Str* name)
{
	// /v1/db/db_name/relation
	if (unlikely(str_empty(self)))
		return false;

	auto pos = self->pos;
	auto end = self->end;

	// /v1/db/
	if (unlikely(! str_is_prefix(self, "/v1/db/", 7)))
		return false;
	pos += 7;

	// db
	auto start = pos;
	while (pos < end && *pos != '/')
		pos++;
	if (unlikely(pos == end))
		return false;
	str_set(db, start, pos - start);
	if (unlikely(str_empty(db)))
		return false;
	pos++;

	// name
	str_set(name, pos, end - pos);
	if (unlikely(str_empty(name)))
		return false;

	return true;
}

static inline bool
parse_endpoint_column(char** pos, char* end, Str* name)
{
	// name
	// name [,]
	auto start = *pos;
	while (*pos < end && **pos != ',')
		(*pos)++;
	str_set(name, start, *pos - start);
	if (*pos != end)
		(*pos)++;
	return !str_empty(name);
}

static void
parse_endpoint_columns(Endpoint* endpoint, Str* value)
{
	endpoint->columns_list_has = true;
	if (str_empty(value))
		return;

	// name[, ...]
	auto pos = value->pos;
	auto end = value->end;

	auto columns = endpoint->columns;
	Ast* last = NULL;

	Str name;
	while (parse_endpoint_column(&pos, end, &name))
	{
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '%.*s' does not exists", str_size(&name),
			      str_of(&name));

		if (last && column->order <= last->column->order)
			error("column list must be ordered");

		Ast* ref = ast(0);
		ref->column = column;
		if (endpoint->columns_list == NULL)
			endpoint->columns_list = ref;
		else
			last->next = ref;
		last = ref;
		endpoint->columns_list_count++;
	}
}

void
parse_endpoint(Endpoint* endpoint)
{
	// POST /v1/db/db_name/relation <?columns=...>
	auto uri = endpoint->uri;

	// get target name
	Str db;
	Str name;
	str_init(&db);
	str_init(&name);
	if (unlikely(! parse_endpoint_path(&uri->path, &db, &name)))
		error("unsupported URI path");

	// find udf
	endpoint->udf = udf_mgr_find(&share()->storage->catalog.udf_mgr, &db, &name, false);
	if (endpoint->udf)
	{
		endpoint->columns = &endpoint->udf->config->args;
		endpoint->values  = NULL;

	} else
	{
		// find table
		endpoint->table   = table_mgr_find(&share()->storage->catalog.table_mgr, &db, &name, true);
		endpoint->columns = &endpoint->table->config->columns;
		endpoint->values  = NULL;
	}

	// validate arguments
	list_foreach(&uri->args)
	{
		auto arg = list_at(UriArg, link);

		// columns=
		if (str_is(&arg->name, "columns", 7))
		{
			if (unlikely(endpoint->columns_list_has))
				error("columns argument redefined");

			parse_endpoint_columns(endpoint, &arg->value);
			continue;
		}

		error("unsupported URI argument '%.*s'", str_size(&arg->name),
		      str_of(&arg->name));
	}
}
#endif

#if 0
hot static inline void
parse_import_row(Stmt* self, Endpoint* endpoint)
{
	// prepare row
	auto row = set_reserve(endpoint->values);

	auto list = endpoint->columns_list;
	list_foreach(&endpoint->columns->list)
	{
		auto column = list_at(Column, link);
		auto column_value = &row[column->order];
		auto column_separator = true;

		Ast* value = NULL;
		if (endpoint->columns_list_has)
		{
			if (list && list->column == column)
			{
				// parse column value
				value = parse_value(self, NULL, column, column_value);
				list = list->next;
				column_separator = list != NULL;
			} else
			{
				// default value, write IDENTITY, RANDOM or DEFAULT
				parse_value_default(self, column, column_value);
				column_separator = false;
			}
		} else
		{
			// parse column value
			value = parse_value(self, NULL, column, column_value);
			column_separator = !list_is_last(&endpoint->columns->list, &column->link);
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
	// prepare row
	auto columns = endpoint->columns;
	auto row = set_reserve(endpoint->values);

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
		auto value = parse_value(self, NULL, column, column_value);

		// ensure NOT NULL constraint and hash key
		parse_value_validate(self, column, column_value, value);

		// ,
		if (! stmt_if(self, ','))
			break;
	}
	if (match_count == columns->count)
		return;

	// default value, write IDENTITY, RANDOM or DEFAULT
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (match[column->order])
			continue;

		auto column_value = &row[column->order];
		parse_value_default(self, column, column_value);

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
		if (endpoint->columns_list_has)
		{
			if (unlikely(endpoint->columns_list_count > 1))
				stmt_error(self, begin, "JSON column list must have zero or one value");

			// process {} as a value for column, if one column
			if (endpoint->columns_list_count == 1)
				stmt_push(self, begin);

			parse_import_row(self, endpoint);
		} else {
			parse_import_obj(self, endpoint);
		}
		if (! endpoint->columns_list_count)
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
	if (endpoint->columns_list_has &&
	    endpoint->columns_list_count == 0)
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
parse_import_table(Parser* self, Endpoint* endpoint)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare insert stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_INSERT;
	stmt->ast = &ast_insert_allocate(block)->ast;
	stmt->is_return = true;

	// create insert target
	auto insert  = ast_insert_of(stmt->ast);
	stmt->ret = &insert->ret;

	auto table   = endpoint->table;
	auto columns = table_columns(table);
	auto target  = target_allocate();
	target->type        = TARGET_TABLE;
	target->ast         = stmt->ast;
	target->from_access = ACCESS_RW;
	target->from_table  = table;
	target->columns     = columns;
	str_set_str(&target->name, &table->config->name);
	from_add(&insert->from, target);
	access_add(&self->program->access, &table->rel, ACCESS_RW);

	// prepare result set
	insert->values   = set_cache_create(self->set_cache, &self->program->sets);
	endpoint->values = insert->values;
	set_prepare(insert->values, columns->count, 0, NULL);

	// parse rows according to the content type
	switch (endpoint->type) {
	case ENDPOINT_CSV:
		parse_import_csv(stmt, endpoint);
		break;
	case ENDPOINT_JSONL:
		parse_import_jsonl(stmt, endpoint);
		break;
	case ENDPOINT_JSON:
		parse_import_json(stmt, endpoint);
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

hot void
parse_import_udf(Parser* self, Endpoint* endpoint)
{
	// create main namespace and the main block
	auto ns    = namespaces_add(&self->nss, NULL, NULL);
	auto block = blocks_add(&ns->blocks, NULL, NULL);

	// prepare execute stmt
	auto stmt = stmt_allocate(self, &self->lex, block);
	stmts_add(&block->stmts, stmt);
	stmt->id  = STMT_EXECUTE;
	stmt->ast = &ast_execute_allocate()->ast;
	stmt->is_return = true;

	auto execute = ast_execute_of(stmt->ast);
	stmt->ret = &execute->ret;

	// prepare arguments
	auto udf = endpoint->udf;
	execute->udf  = udf;
	execute->args = set_cache_create(self->set_cache, &self->program->sets);
	endpoint->values = execute->args;
	set_prepare(execute->args, udf->config->args.count, 0, NULL);

	// parse rows according to the content type
	switch (endpoint->type) {
	case ENDPOINT_CSV:
		parse_import_row(stmt, endpoint);
		break;
	case ENDPOINT_JSONL:
	case ENDPOINT_JSON:
		parse_import_json_row(stmt, endpoint);
		break;
	}

	// ensure there are no data left
	if (! stmt_if(stmt, KEOF))
		stmt_error(stmt, NULL, "eof expected");

	// set returning column
	if (udf->config->type != TYPE_NULL)
	{
		auto column = column_allocate();
		column_set_name(column, &udf->config->name);
		column_set_type(column, udf->config->type, -1);
		columns_add(&execute->ret.columns, column);
	}
}

hot void
parse_import(Parser* self, Program* program, Str* str, Str* uri,
             EndpointType type)
{
	self->program = program;

	// prepare parser
	lex_start(&self->lex, str);

	// parse uri and get the argument list
	uri_set(&self->uri, uri, true);

	// parse endpoint path and arguments
	Endpoint endpoint;
	endpoint_init(&endpoint, &self->uri, type);
	parse_endpoint(&endpoint);
	if (endpoint.table)
		parse_import_table(self, &endpoint);
	else
		parse_import_udf(self, &endpoint);
}
#endif
