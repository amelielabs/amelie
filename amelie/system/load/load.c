
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
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_load.h>

void
load_init(Load* self, Share* share, Dtr* dtr)
{
	self->columns_has   = false;
	self->columns_count = 0;
	self->table         = NULL;
	self->dtr           = dtr;
	self->request       = NULL;
	self->content_type  = NULL;
	self->share         = share;
	buf_init(&self->columns);
	req_list_init(&self->req_list);
	json_init(&self->json);
	uri_init(&self->uri);
}

void
load_free(Load* self)
{
	buf_free(&self->columns);
	json_free(&self->json);
	uri_free(&self->uri);
}

void
load_reset(Load* self)
{
	self->columns_has   = false;
	self->columns_count = 0;
	self->table         = NULL;
	self->request       = NULL;
	self->content_type  = NULL;
	buf_reset(&self->columns);
	req_list_init(&self->req_list);
	json_reset(&self->json);
	uri_reset(&self->uri);
}

static inline bool
load_target(Str* self, Str* schema, Str* name)
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
load_column(char** pos, char* end, Str* name)
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
load_columns(Load* self, Str* value)
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
	while (load_column(&pos, end, &name))
	{
		auto column = columns_find(columns, &name);
		if (! column)
			error("column '%.*s' does not exists", str_size(&name),
			      str_of(&name));

		if (unlikely(column->constraint.generated == GENERATED_VIRTUAL))
			error("virtual columns cannot be updated");

		if (last && column->order <= last->order)
			error("column list must be ordered");

		buf_write(&self->columns, &column, sizeof(void**));
		self->columns_count++;

		last = column;
	}
}

void
load_prepare(Load* self, Http* request)
{
	self->request = request;

	// POST /schema/table <?columns=...>
	auto url = &request->options[HTTP_URL];
	uri_set(&self->uri, url, true);

	// get content type
	self->content_type = http_find(request, "Content-Type", 12);
	if (unlikely(! self->content_type))
		error("Content-Type is missing");

	// get target name
	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);
	if (unlikely(! load_target(&self->uri.path, &schema, &name)))
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

			load_columns(self, &arg->value);
			continue;
		}

		error("unsupported argument '%.*s'", str_size(&arg->name),
		      str_of(&arg->name));
	}
}

void
load_run(Load* self)
{
	auto executor = self->share->executor;
	auto dtr = self->dtr;

	Program program;
	program_init(&program);
	program.stmts      = 1;
	program.stmts_last = 0;
	program.ctes       = 1;

	dtr_reset(dtr);
	dtr_create(dtr, &program, NULL);

	Exception e;
	if (enter(&e))
	{
		if (str_is(&self->content_type->value, "application/jsonl", 17))
			load_jsonl(self);
		else
		if (str_is(&self->content_type->value, "application/json", 16))
			load_json(self);
		else
		if (str_is(&self->content_type->value, "text/csv", 8))
			load_csv(self);
		else
			error("unsupported API operation");

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
