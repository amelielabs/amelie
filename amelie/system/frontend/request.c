
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>

void
request_init(Request* self)
{
	self->type = REQUEST_UNDEF;
	self->args = NULL;
	self->user = NULL;
	self->lock = NULL;
	str_init(&self->text);
	str_init(&self->rel_user);
	str_init(&self->rel);
	endpoint_init(&self->endpoint);
	output_init(&self->output);
	jsonrpc_init(&self->jsonrpc);
}

void
request_free(Request* self)
{
	request_reset(self);
	endpoint_free(&self->endpoint);
	jsonrpc_free(&self->jsonrpc);
}

void
request_reset(Request* self)
{
	self->user = NULL;
	self->type = REQUEST_UNDEF;
	self->args = NULL;
	str_init(&self->text);
	str_init(&self->rel_user);
	str_init(&self->rel);
	request_unlock(self);
	endpoint_reset(&self->endpoint);
	output_reset(&self->output);
	jsonrpc_reset(&self->jsonrpc);
}

void
request_lock(Request* self, LockId id)
{
	// take catalog lock
	if (self->lock)
	{
		if (self->lock->rel_lock == id)
			return;
		unlock(self->lock);
		self->lock = NULL;
	}
	self->lock = lock_system(REL_CATALOG, id);
	if (self->lock)
		lock_detach(self->lock);
}

void
request_unlock(Request* self)
{
	if (self->lock)
	{
		unlock(self->lock);
		self->lock = NULL;
	}
}

hot void
request_auth(Request* self, Auth* auth_ref)
{
	// take catalog lock
	self->lock = lock_system(REL_CATALOG, LOCK_SHARED);
	lock_detach(self->lock);

	// authenticate user
	auto endpoint = &self->endpoint;
	auto token = &endpoint->token.string;
	auto token_required = opt_int_of(&endpoint->auth);
	auto user_id = &endpoint->user.string;
	self->user = auth(auth_ref, user_id, token, token_required);
}

void
request_rpc(Request* self, Str* content)
{
	// parse jsonrpc request
	jsonrpc_parse(&self->jsonrpc, content);
	if (unlikely(self->jsonrpc.reqs_count > 1))
		error("jsonrpc batching is not supported");

	// todo: set endpoint id

	// read command
	auto cmd = jsonrpc_first(&self->jsonrpc);
	if (str_is(&cmd->method, "sql", 3))
	{
		// sql
		self->type = REQUEST_SQL;

		auto pos = cmd->params;
		if (unlikely(! pos))
			error("sql 'params' are missing");

		if (! json_is_obj(pos))
			error("sql 'params' is not an object");

		// parse params
		json_read_obj(&pos);
		while (! json_read_obj_end(&pos))
		{
			Str name;
			json_read_string(&pos, &name);
			if (str_is(&name, "text", 4))
			{
				if (unlikely(! json_is_string(pos)))
					error("sql 'text' is not a string");
				json_read_string(&pos, &self->text);
			} else {
				error("unexpected params field");
			}
		}

		if (str_empty(&self->text))
			error("sql 'text' is not defined");
	} else
	{
		error("unknown method");
	}
}
