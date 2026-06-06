
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
api_init(Api* self, Request* req)
{
	self->type    = API_UNDEF;
	self->args    = NULL;
	self->request = req;
	str_init(&self->text);
	str_init(&self->rel_user);
	str_init(&self->rel);
	jsonrpc_init(&self->jsonrpc);
}

void
api_free(Api* self)
{
	jsonrpc_free(&self->jsonrpc);
}

void
api_reset(Api* self)
{
	self->type = API_UNDEF;
	self->args = NULL;
	str_init(&self->text);
	str_init(&self->rel_user);
	str_init(&self->rel);
	jsonrpc_reset(&self->jsonrpc);
}

static void
api_parse_sql(Api* self)
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
		unpack_str(&pos, &name);
		if (str_is(&name, "text", 4))
		{
			if (unlikely(! data_is_str(pos)))
				error("'text' field is not a string");
			unpack_str(&pos, &self->text);
		} else {
			error("unexpected params field");
		}
	}

	if (str_empty(&self->text))
		error("'text' field is not defined");

	self->type = API_SQL;
}

static void
api_parse_cmd(Api* self, ApiType type, bool with_args)
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
		unpack_str(&pos, &name);
		if (str_is(&name, "user", 4))
		{
			if (unlikely(! data_is_str(pos)))
				error("'user' is not a string");
			unpack_str(&pos, &self->rel_user);
		} else
		if (str_is(&name, "rel", 3))
		{
			if (unlikely(! data_is_str(pos)))
				error("'rel' is not a string");
			unpack_str(&pos, &self->rel);
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

static void
api_parse_content(Api* self, Str* content)
{
	auto jsonrpc = &self->jsonrpc;

	// parse jsonrpc request
	jsonrpc_parse(jsonrpc, content);
	if (unlikely(jsonrpc->reqs_count > 1))
		error("jsonrpc batching is not supported");

	// read command
	auto cmd = jsonrpc_first(jsonrpc);

	// set endpoint id
	opt_json_set_data(&self->request->endpoint.id, cmd->id);

	// sql
	if (str_is(&cmd->method, "sql", 3))
	{
		api_parse_sql(self);
		return;
	}

	// write, ack
	if (str_is(&cmd->method, "write", 5) ||
	    str_is(&cmd->method, "ack", 3))
	{
		api_parse_cmd(self, API_WRITE, true);
		return;
	}

	// follow
	if (str_is(&cmd->method, "follow", 6))
	{
		api_parse_cmd(self, API_FOLLOW, false);
		return;
	}

	// unfollow
	if (str_is(&cmd->method, "unfollow", 8))
	{
		api_parse_cmd(self, API_UNFOLLOW, false);
		return;
	}

	error("unknown jsonrpc method: {str}", &cmd->method);
}

bool
api_parse(Api* self, Str* content, Query* query, bool subscribe)
{
	// parser jsonrpc request
	auto on_error = error_catch
	(
		api_parse_content(self, content);
		if (! subscribe)
			if (self->type == API_FOLLOW || self->type == API_UNFOLLOW)
				error("websocket connection required to follow");
	);
	if (on_error)
	{
		output_error(&self->request->output, &am_self()->error);
		return false;
	}

	// prepare query
	switch (self->type) {
	case API_SQL:
	{
		query->type     = QUERY_SQL;
		query->text     = &self->text;
		break;
	}
	case API_WRITE:
	{
		query->type     = QUERY_WRITE;
		query->rel_user = &self->rel_user;
		query->rel      = &self->rel;
		query->args     = self->args;
		break;
	}
	default:
		query->type     = QUERY_UNDEF;
		break;
	}
	return true;
}
