
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
	// { }
	auto cmd = jsonrpc_first(&self->jsonrpc);
	auto pos = cmd->params;
	if (unlikely(! pos))
		error("'params' field is missing");
	if (! data_is_obj(pos))
		error("'params' field is not an object");

	// todo: validate params
	self->type = MCP_INITIALIZE;
}

static void
mcp_parse_tools_list(Mcp* self)
{
	// todo: validate params
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
