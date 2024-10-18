
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
	self->table   = NULL;
	self->dtr     = NULL;
	self->request = NULL;
	self->local   = local;
	self->share   = share;
	req_list_init(&self->req_list);
	json_init(&self->json);
	uri_init(&self->uri);
}

void
import_free(Import* self)
{
	json_free(&self->json);
	uri_free(&self->uri);
}

void
import_reset(Import* self)
{
	self->table   = NULL;
	self->dtr     = NULL;
	self->request = NULL;
	req_list_init(&self->req_list);
	json_reset(&self->json);
	uri_reset(&self->uri);
}

static inline bool
import_prepare_target(Str* self, Str* schema, Str* name)
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

void
import_prepare(Import* self, Dtr* dtr, Http* request)
{
	self->dtr     = dtr;
	self->request = request;

	// POST /schema/table <&columns=...>
	auto url = &request->options[HTTP_URL];
	uri_set(&self->uri, url, true);

	// get target name
	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);
	if (unlikely(! import_prepare_target(&self->uri.path, &schema, &name)))
		error("unsupported API target path");

	// find table
	self->table = table_mgr_find(&self->share->db->table_mgr, &schema, &name, true);

	// validate options
	Str* columns = NULL;

	// validate column list
		// columns must be in order

	(void)columns;

	// validate content type
	auto type = http_find(request, "Content-Type", 12);
	if (unlikely(! type))
		error("Content-Type is missing");

	if (! str_is(&type->value, "application/json", 16))
		error("unsupported API operation");
}

static inline bool
import_skip_ws(char** pos, char* end)
{
	while (*pos < end && isspace(**pos))
		(*pos)++;
	return (*pos == end);
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

	// [
	char* pos = buf_cstr(&self->request->content);
	char* end = pos + buf_size(&self->request->content);
	if (import_skip_ws(&pos, end) || *pos != '[')
		error("JSON array expected");
	pos++;

	for (;;)
	{
		// set next serial value
		// uint64_t serial = serial_next(&table->serial);
		// (void)serial;

		// [
		if (import_skip_ws(&pos, end) || *pos != '[')
			error("JSON array expected");
		pos++;

		uint32_t offset_row = buf_size(&self->json.buf_data);
		uint32_t hash       = 0;
		encode_array(&self->json.buf_data);

		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);

			// read json value
			uint32_t offset = buf_size(&self->json.buf_data);
			Str in;
			str_set(&in, pos, end - pos);
			json_set_time(&self->json, self->local->timezone, self->local->time_us);
			json_parse(&self->json, &in, NULL);
			pos = self->json.pos;

			if (column->key)
			{
				list_foreach(&keys->list)
				{
					auto key = list_at(Key, link);
					if (key->column != column)
						continue;

					// find key path and validate data type
					uint8_t* pos_key = self->json.buf_data.start + offset;
					key_find(key, &pos_key);

					// hash key
					hash = key_hash(hash, pos_key);
				}
			}

			if (import_skip_ws(&pos, end))
				error("failed to parse JSON array");

			// ]
			if (list_is_last(&columns->list, &column->link))
			{
				if (*pos != ']')
					error("failed to parse JSON array");
				pos++;
				encode_array_end(&self->json.buf_data);
				continue;
			}

			// ,
			if (*pos != ',')
				error("failed to parse JSON array");
			pos++;
		}

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

		// write u32 offset to req->arg
		encode_integer(&req->arg, offset_row);

		// ] or ,
		if (import_skip_ws(&pos, end))
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

#if 0
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
			parse_value(self);

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
#endif

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
