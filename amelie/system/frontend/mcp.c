
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
mcp_init(Mcp* self, Request* req)
{
	self->type    = MCP_UNDEF;
	self->args    = NULL;
	self->request = req;
	str_init(&self->rel_user);
	str_init(&self->rel);
	jsonrpc_init(&self->jsonrpc);
}

void
mcp_free(Mcp* self)
{
	jsonrpc_free(&self->jsonrpc);
}

void
mcp_reset(Mcp* self)
{
	self->type = MCP_UNDEF;
	self->args = NULL;
	str_init(&self->rel_user);
	str_init(&self->rel);
	jsonrpc_reset(&self->jsonrpc);
}

static void
mcp_parse_initialize(Mcp* self)
{
	// (params are not validated)
	self->type = MCP_INITIALIZE;
}

static void
mcp_parse_tools_list(Mcp* self)
{
	// (params are not validated)
	self->type = MCP_TOOLS_LIST;
}

static void
mcp_parse_tools_call(Mcp* self)
{
	// { name, aruments }
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
		if (str_is(&name, "name", 4))
		{
			if (unlikely(! data_is_str(pos)))
				error("'name' is not a string");
			unpack_str(&pos, &self->rel);
		} else
		if (str_is(&name, "arguments", 9))
		{
			if (unlikely(!data_is_array(pos) && !data_is_obj(pos)))
				error("'arguments' is not an array or object");
			self->args = pos;
			data_skip(&pos);
		} else {
			// skipping unknown field
			data_skip(&pos);
		}
	}

	// validate options
	if (str_empty(&self->rel))
		error("'name' is not defined");

	if (! self->args)
		error("'arguments' is not defined");

	self->type = MCP_TOOLS_CALL;
}

static void
mcp_parse_resources_list(Mcp* self)
{
	// (params are not validated)
	self->type = MCP_RESOURCES_LIST;
}

static bool
mcp_parse_uri(Mcp* self, Str* uri)
{
	// amelie://user/rel
	if (! str_is_prefix(uri, "amelie://", 9))
		return false;

	// user
	auto start = uri->pos + 9;
	auto pos   = start;
	auto end   = uri->end;
	while (pos < end && *pos != '/')
		pos++;
	str_set(&self->rel_user, start, pos - start);
	if (str_empty(&self->rel_user))
		return false;
	if (pos == uri->end)
		return false;
	pos++;

	// rel
	start = pos;
	while (pos < end && *pos != '/')
		pos++;
	str_set(&self->rel, start, pos - start);
	if (str_empty(&self->rel))
		return false;

	// [/]
	if (pos == end)
		return true;
	pos++;
	return pos == end;
}

static void
mcp_parse_resources_read(Mcp* self)
{
	// { uri }
	auto cmd = jsonrpc_first(&self->jsonrpc);
	auto pos = cmd->params;
	if (unlikely(! pos))
		error("'params' are missing");
	if (! data_is_obj(pos))
		error("'params' is not an object");

	Str uri;
	str_init(&uri);

	// parse params
	unpack_obj(&pos);
	while (! unpack_obj_end(&pos))
	{
		Str name;
		unpack_str(&pos, &name);
		if (str_is(&name, "uri", 3))
		{
			if (unlikely(! data_is_str(pos)))
				error("'uri' is not a string");
			unpack_str(&pos, &uri);
		} else {
			// skipping unknown field
			data_skip(&pos);
		}
	}

	// validate options
	if (str_empty(&uri))
		error("'uri' is not defined");

	if (! mcp_parse_uri(self, &uri))
		error("'uri' is not valid");

	self->type = MCP_RESOURCES_READ;
}

static void
mcp_parse_content(Mcp* self, Str* content)
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

	// initialize
	if (str_is(&cmd->method, "initialize", 10))
		return mcp_parse_initialize(self);

	// tools/list
	if (str_is(&cmd->method, "tools/list", 10))
		return mcp_parse_tools_list(self);

	// tools/call
	if (str_is(&cmd->method, "tools/call", 10))
		return mcp_parse_tools_call(self);

	// resources/list
	if (str_is(&cmd->method, "resources/list", 14))
		return mcp_parse_resources_list(self);

	// resources/read
	if (str_is(&cmd->method, "resources/read", 14))
		return mcp_parse_resources_read(self);

	error("unknown jsonrpc method: {str}", &cmd->method);
}

bool
mcp_parse(Mcp* self, Str* content, Query* query)
{
	// parser jsonrpc request
	auto on_error = error_catch
	(
		mcp_parse_content(self, content);
	);
	if (on_error)
	{
		output_error(&self->request->output, &am_self()->error);
		return false;
	}

	// execute mcp request or prepare query
	mcp_execute(self, query);
	return true;
}
