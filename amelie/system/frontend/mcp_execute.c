
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

static void
mcp_initialize(Mcp* self)
{
	auto req = self->request;

	// {}
	auto buf = buf_create();
	defer_buf(buf);
	encode_obj(buf);

	// jsonrpc
	encode_raw(buf, "jsonrpc", 7);
	encode_raw(buf, "2.0", 3);

	// id
	encode_raw(buf, "id", 2);
	auto id = &req->endpoint.id.string;
	if (str_empty(id))
		encode_null(buf);
	else
		buf_write_str(buf, id);

	// result {}
	encode_raw(buf, "result", 6);
	encode_obj(buf);

	// protocolVersion
	encode_raw(buf, "protocolVersion", 15);
	encode_raw(buf, "2024-11-05", 10);

	// capabilities
	encode_raw(buf, "capabilities", 12);
	encode_obj(buf);

	// tools {}
	encode_raw(buf, "tools", 5);
	encode_obj(buf);
	encode_obj_end(buf);
	encode_obj_end(buf);

	// serverInfo {}
	encode_raw(buf, "serverInfo", 10);
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_raw(buf, "amelie", 6);

	// version
	encode_raw(buf, "version", 7);
	encode_str(buf, &state()->version.string);
	encode_obj_end(buf);

	encode_obj_end(buf);
	encode_obj_end(buf);

	auto pos = buf->start;
	json_export_as(req->output.buf, req->output.timezone, true, 0, &pos);
}

static void
mcp_tools_list(Mcp* self)
{
	auto req = self->request;

	// {}
	auto buf = buf_create();
	defer_buf(buf);
	encode_obj(buf);

	// jsonrpc
	encode_raw(buf, "jsonrpc", 7);
	encode_raw(buf, "2.0", 3);

	// id
	encode_raw(buf, "id", 2);
	auto id = &req->endpoint.id.string;
	if (str_empty(id))
		encode_null(buf);
	else
		buf_write_str(buf, id);

	// result {}
	encode_raw(buf, "result", 6);
	encode_obj(buf);

	// tools []
	encode_raw(buf, "tools", 5);
	catalog_mcp_tools(&share()->db->catalog, &req->user->config->name, buf);

	encode_obj_end(buf);
	encode_obj_end(buf);

	auto pos = buf->start;
	json_export_as(req->output.buf, req->output.timezone, true, 0, &pos);
}

static void
output_mcp_write(Output* self, Columns* columns, Value* value)
{
	char header[] = "{\"jsonrpc\": \"2.0\", \"id\": ";
	auto buf = self->buf;
	buf_write(buf, header, sizeof(header) - 1);

	auto id  = &self->endpoint->id.string;
	auto pos = str_u8(id);
	if (str_empty(id))
		buf_write(buf, "null", 4);
	else
		json_export_as(buf, self->timezone, false, 0, &pos);

	// result {}
	char result[] = ", \"result\": {\"content\": ";
	buf_write(buf, result, sizeof(result) - 1);

	char content[] = "[{\"type\": \"text\", \"text\": \"";
	buf_write(buf, content, sizeof(content) - 1);

	// emit text result
	auto text = buf_create();
	defer_buf(text);

	auto print = &self->print;
	print_create(print, columns, value, self->timezone, text);
	print_run(print);

	Str text_str;
	buf_str(text, &text_str);
	escape_str(buf, &text_str);

	char content_end[] = "\"}], \"isError\": false}";
	buf_write(buf, content_end, sizeof(content_end) - 1);

	buf_write(self->buf, "}", 1);
}

static void
output_mcp_write_error(Output* self, Error* error)
{
	Mcp* mcp = self->iface_arg;
	if (mcp->type != MCP_TOOLS_CALL)
	{
		output_jsonrpc.write_error(self, error);
		return;
	}

	char header[] = "{\"jsonrpc\": \"2.0\", \"id\": ";
	auto buf = self->buf;
	buf_write(buf, header, sizeof(header) - 1);

	auto id  = &self->endpoint->id.string;
	auto pos = str_u8(id);
	if (str_empty(id))
		buf_write(buf, "null", 4);
	else
		json_export_as(buf, self->timezone, false, 0, &pos);

	// result {}
	char result[] = ", \"result\": {\"content\": ";
	buf_write(buf, result, sizeof(result) - 1);

	char content[] = "[{\"type\": \"text\", \"text\": \"";
	buf_write(buf, content, sizeof(content) - 1);

	// error message
	auto text = buf_create();
	defer_buf(text);
	buf_write(text, "error: ", 7);

	Str text_error;
	buf_str(&error->text, &text_error);

	// truncate large error msg
	const int text_max = 512;
	if (str_size(&text_error) > text_max)
	{
		char cut[] = "…\n";
		buf_write(text, cut, sizeof(cut) - 1);
		buf_write(text, text_error.end - text_max, text_max);
	} else
	{
		buf_write_str(text, &text_error);
	}
	Str text_str;
	buf_str(text, &text_str);
	escape_str(buf, &text_str);

	char content_end[] = "\"}], \"isError\": true}";
	buf_write(buf, content_end, sizeof(content_end) - 1);

	buf_write(self->buf, "}", 1);
}

static void
output_mcp_write_none(Output* self)
{
	output_jsonrpc.write_none(self);
}

OutputIf output_mcp =
{
	.write       = output_mcp_write,
	.write_str   = NULL,
	.write_data  = NULL,
	.write_error = output_mcp_write_error,
	.write_none  = output_mcp_write_none
};

void
mcp_execute(Mcp* self, Query* query)
{
	// process request
	query->type = QUERY_UNDEF;
	switch (self->type) {
	case MCP_INITIALIZE:
	{
		mcp_initialize(self);
		break;
	}
	case MCP_TOOLS_LIST:
	{
		mcp_tools_list(self);
		break;
	}
	case MCP_TOOLS_CALL:
	{
		// execute UDF call as query
		query->type = QUERY_WRITE;
		query->rel  = &self->rel;
		query->args = self->args;

		auto output = &self->request->output;
		output->iface_arg = self;
		output->iface     = &output_mcp;
		break;
	}
	default:
		break;
	}
}
