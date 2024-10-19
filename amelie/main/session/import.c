
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>

void
import_init(Import* self, Share* share, Local* local)
{
	self->table         = NULL;
	self->columns_has   = false;
	self->columns_count = 0;
	self->dtr           = NULL;
	self->request       = NULL;
	self->local         = local;
	self->share         = share;
	buf_init(&self->columns);
	req_list_init(&self->req_list);
	json_init(&self->json);
	uri_init(&self->uri);
}

void
import_free(Import* self)
{
	buf_free(&self->columns);
	json_free(&self->json);
	uri_free(&self->uri);
}

void
import_reset(Import* self)
{
	self->table         = NULL;
	self->columns_has   = false;
	self->columns_count = 0;
	self->dtr           = NULL;
	self->request       = NULL;
	buf_reset(&self->columns);
	req_list_init(&self->req_list);
	json_reset(&self->json);
	uri_reset(&self->uri);
}

static inline bool
import_target(Str* self, Str* schema, Str* name)
{
	if (unlikely(str_empty(self)))
		return false;

	// /schema/table
	auto pos = self->pos;
	auto end = self->end;

	// /
	if (unlikely(*pos != '/'))
		return false;
	pos++;

	// schema
	auto start = pos;
	while (pos < end && *pos != '/')
		pos++;
	if (unlikely(pos == end))
		return false;
	str_set(schema, start, pos - start);
	if (unlikely(str_empty(schema)))
		return false;
	pos++;

	// name
	str_set(name, pos, end - pos);
	if (unlikely(str_empty(name)))
		return false;

	return true;
}

static inline bool
import_column(char** pos, char* end, Str* name)
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

static inline void
import_columns(Import* self, Str* value)
{
	self->columns_has = true;
	if (str_empty(value))
		return;

	// name[, ...]
	auto pos = value->pos;
	auto end = value->end;

	auto columns = table_columns(self->table);
	Column* last = NULL;

	Str name;
	while (import_column(&pos, end, &name))
	{
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '%.*s' does not exists", str_size(&name),
			      str_of(&name));

		if (last && column->order <= last->order)
			error("column list must be ordered");

		buf_write(&self->columns, &column, sizeof(void**));
		self->columns_count++;

		last = column;
	}
}

void
import_prepare(Import* self, Dtr* dtr, Http* request)
{
	self->dtr     = dtr;
	self->request = request;

	// POST /schema/table <?columns=...>
	auto url = &request->options[HTTP_URL];
	uri_set(&self->uri, url, true);

	// get target name
	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);
	if (unlikely(! import_target(&self->uri.path, &schema, &name)))
		error("unsupported API target path");

	// find table
	self->table = table_mgr_find(&self->share->db->table_mgr, &schema, &name, true);

	// validate arguments
	list_foreach(&self->uri.args)
	{
		auto arg = list_at(UriArg, link);

		// columns=
		if (str_is(&arg->name, "columns", 7))
		{
			if (unlikely(self->columns_has))
				error("columns redefined");

			import_columns(self, &arg->value);
			continue;
		}

		error("unsupported argument '%.*s'", str_size(&arg->name),
		      str_of(&arg->name));
	}

	// validate content type
	auto type = http_find(request, "Content-Type", 12);
	if (unlikely(! type))
		error("Content-Type is missing");

	if (! str_is(&type->value, "application/json", 16))
		error("unsupported API operation");
}

static inline bool
import_skip(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return (*pos == end);
}

static inline char*
import_value(Import* self, char* pos, char* end, uint32_t* offset)
{
	// read json value
	*offset = buf_size(&self->json.buf_data);
	Str in;
	str_set(&in, pos, end - pos);
	json_set_time(&self->json, self->local->timezone, self->local->time_us);
	json_parse(&self->json, &in, NULL);
	return self->json.pos;
}

hot static void
import_main(Import* self)
{
	auto dtr     = self->dtr;
	auto table   = self->table;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	// [[value, ...], ...]
	char* pos = buf_cstr(&self->request->content);
	char* end = pos + buf_size(&self->request->content);
	if (import_skip(&pos, end) || *pos != '[')
		error("JSON array expected");
	pos++;

	// prepare column list
	auto list = (Column**)(self->columns.start);
	int  list_pos = 0;
	for (;;)
	{
		// [value, ...]
		if (import_skip(&pos, end) || *pos != '[')
			error("JSON array expected");
		pos++;

		uint32_t offset_row = buf_size(&self->json.buf_data);
		uint32_t hash       = 0;
		encode_array(&self->json.buf_data);

		// reset column list
		list_pos = 0;

		// set next serial value
		uint64_t serial = serial_next(&table->serial);

		list_foreach(&columns->list)
		{
			auto     column = list_at(Column, link);
			auto     separator = false;
			auto     separator_last = false;

			uint32_t offset;
			if (self->columns_has)
			{
				if (list_pos < self->columns_count && list[list_pos]->order == column->order)
				{
					// parse and encode json value which matches column list
					pos = import_value(self, pos, end, &offset);

					list_pos++;
					separator = true;
				} else
				{
					// null or default
					offset = buf_size(&self->json.buf_data);

					// GENERATED
					auto cons = &column->constraint;
					if (cons->generated == GENERATED_SERIAL)
					{
						encode_integer(&self->json.buf_data, serial);
					} else
					if (cons->generated == GENERATED_RANDOM)
					{
						auto value = random_generate(global()->random) % cons->modulo;
						encode_integer(&self->json.buf_data, value);
					} else
					{
						// use DEFAULT (NULL by default)
						buf_write(&self->json.buf_data, cons->value.start, buf_size(&cons->value));
					}
				}

				// force ] check
				if (list_pos >= self->columns_count)
				{
					separator = true;
					separator_last = true;
				}

			} else
			{
				// parse and encode json value
				pos = import_value(self, pos, end, &offset);

				separator = true;
				separator_last = list_is_last(&columns->list, &column->link);
			}

			// ] or ,
			if (separator)
			{
				if (unlikely(import_skip(&pos, end)))
					error("failed to parse JSON array");
				if (unlikely(*pos != ']' && *pos != ','))
					error("failed to parse JSON array");
				if (unlikely((*pos == ']' && !separator_last) ||
				             (*pos == ',' &&  separator_last)))
					error("array expected to have %d columns",
					      columns->list_count);
				pos++;
			}

			// ensure NOT NULL constraint
			if (column->constraint.not_null)
			{
				uint8_t* ref = self->json.buf_data.start + offset;
				if (data_is_null(ref))
					error("column <%.*s> value cannot be NULL", str_size(&column->name),
					      str_of(&column->name));
			}

			// use primary key to calculate hash
			if (column->key)
			{
				list_foreach(&keys->list)
				{
					auto key = list_at(Key, link);
					if (key->column != column)
						continue;

					// find key path and validate data type
					uint8_t* ref = self->json.buf_data.start + offset;
					key_find(key, &ref);

					// hash key
					hash = key_hash(hash, ref);
				}
			}
		}

		encode_array_end(&self->json.buf_data);

		// map to node
		auto route = part_map_get(&table->part_list.map, hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_IMPORT);
			req->arg_import = &self->json.buf_data;
			req->arg_import_table = self->table;
			req->route = route;
			req_list_add(&self->req_list, req);
			map[route->order] = req;
		}

		// write offset to req->arg
		encode_integer(&req->arg, offset_row);

		// ] or ,
		if (import_skip(&pos, end))
			error("failed to parse JSON array");
		if (*pos == ',') {
			pos++;
			continue;
		}
		if (*pos == ']') {
			pos++;
			break;
		}
		error("failed to parse JSON array");
	}
}

void
import_run(Import* self)
{
	auto executor = self->share->executor;
	auto dtr = self->dtr;

	Program program;
	program_init(&program);
	program.stmts      = 1;
	program.stmts_last = 0;
	program.ctes       = 1;

	dtr_reset(dtr);
	dtr_create(dtr, self->local, &program, NULL);

	Exception e;
	if (enter(&e))
	{
		import_main(self);

		executor_send(executor, dtr, 0, &self->req_list);
		executor_recv(executor, dtr, 0);
	}

	Buf* error = NULL;
	if (leave(&e))
	{
		req_list_free(&self->req_list);
		req_list_init(&self->req_list);
		error = msg_error(&am_self()->error);
	}

	executor_commit(executor, dtr, error);
}
