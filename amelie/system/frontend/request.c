
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
	request_reset(self, true);
	endpoint_free(&self->endpoint);
	jsonrpc_free(&self->jsonrpc);
}

void
request_reset(Request* self, bool with_endpoint)
{
	request_unlock(self);

	self->user = NULL;
	self->type = REQUEST_UNDEF;
	self->args = NULL;
	str_init(&self->text);
	str_init(&self->rel_user);
	str_init(&self->rel);
	if (with_endpoint)
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

static void
request_rpc_sql(Request* self)
{
	// { text }
	auto cmd = jsonrpc_first(&self->jsonrpc);
	auto pos = cmd->params;
	if (unlikely(! pos))
		error("'params' field is missing");
	if (! data_is_obj(pos))
		error("'params' field is not an object");

	// parse params
	unpack_obj(&pos);
	while (! unpack_obj_end(&pos))
	{
		Str name;
		unpack_string(&pos, &name);
		if (str_is(&name, "text", 4))
		{
			if (unlikely(! data_is_string(pos)))
				error("'text' field is not a string");
			unpack_string(&pos, &self->text);
		} else {
			error("unexpected params field");
		}
	}

	if (str_empty(&self->text))
		error("'text' field is not defined");

	self->type = REQUEST_SQL;
}

static void
request_rpc_cmd(Request* self, RequestType type, bool with_args)
{
	// { [user], rel, args }
	auto cmd = jsonrpc_first(&self->jsonrpc);
	auto pos = cmd->params;
	if (unlikely(! pos))
		error("'params' are missing");
	if (! data_is_obj(pos))
		error("'params' is not an object");

	// parse params
	unpack_obj(&pos);
	while (! unpack_obj_end(&pos))
	{
		Str name;
		unpack_string(&pos, &name);
		if (str_is(&name, "user", 4))
		{
			if (unlikely(! data_is_string(pos)))
				error("'user' is not a string");
			unpack_string(&pos, &self->rel_user);
		} else
		if (str_is(&name, "rel", 3))
		{
			if (unlikely(! data_is_string(pos)))
				error("'rel' is not a string");
			unpack_string(&pos, &self->rel);
		} else
		if (str_is(&name, "args", 4))
		{
			if (unlikely(!data_is_array(pos) && !data_is_obj(pos)))
				error("'args' is not an array or object");
			self->args = pos;
			data_skip(&pos);
		} else {
			error("unexpected params field");
		}
	}

	// validate options
	if (str_empty(&self->rel))
		error("'rel' is not defined");

	if (with_args)
	{
		if (! self->args)
			error("'args' is not defined");
	} else
	if (self->args)
		error("'args' is not expected for this command");

	self->type = type;
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

	// sql
	if (str_is(&cmd->method, "sql", 3))
		return request_rpc_sql(self);

	// write, ack, insert, publish, execute
	if (str_is(&cmd->method, "write", 5)   ||
	    str_is(&cmd->method, "ack", 3)     ||
	    str_is(&cmd->method, "insert", 6)  ||
	    str_is(&cmd->method, "publish", 7) ||
	    str_is(&cmd->method, "execute", 7))
		return request_rpc_cmd(self, REQUEST_WRITE, true);

	// follow
	if (str_is(&cmd->method, "follow", 6))
		return request_rpc_cmd(self, REQUEST_FOLLOW, false);

	// unfollow
	if (str_is(&cmd->method, "unfollow", 8))
		return request_rpc_cmd(self, REQUEST_UNFOLLOW, false);

	error("unknown jsonrpc method: %.*s", str_size(&cmd->method),
	      str_of(&cmd->method));
}
