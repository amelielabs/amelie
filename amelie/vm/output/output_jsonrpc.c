
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
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

static void
output_jsonrpc_begin(Output* self)
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

	char result[] = ", \"result\": ";
	buf_write(buf, result, sizeof(result) - 1);
}

static void
output_jsonrpc_write(Output* self, Columns* columns, Value* value)
{
	output_jsonrpc_begin(self);
	output_json_result(self, columns, value);
	buf_write(self->buf, "}", 1);
}

static void
output_jsonrpc_write_str(Output* self, Str* column, Str* str)
{
	output_jsonrpc_begin(self);
	output_json_result_str(self, column, str);
	buf_write(self->buf, "}", 1);
}

static void
output_jsonrpc_write_data(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	output_jsonrpc_begin(self);
	output_json_result_json(self, column, pos, unwrap);
	buf_write(self->buf, "}", 1);
}

static void
output_jsonrpc_write_error(Output* self, Error* error)
{
	auto buf = buf_create();
	defer_buf(buf);

	// {}
	encode_obj(buf);

	// jsonrpc
	encode_raw(buf, "jsonrpc", 7);
	encode_raw(buf, "2.0", 3);

	// id
	encode_raw(buf, "id", 2);
	auto id = &self->endpoint->id.string;
	if (str_empty(id))
		encode_null(buf);
	else
		buf_write_str(buf, id);

	// error
	encode_raw(buf, "error", 5);
	encode_obj(buf);

	// code
	encode_raw(buf, "code", 4);
	encode_int(buf, -32000);

	// message
	encode_raw(buf, "message", 7);

	// truncate large error msg
	const auto text_max = 512;
	Str text;
	buf_str(&error->text, &text);
	if (str_size(&text) > text_max)
	{
		encode_str32(buf, 4 + text_max);
		buf_reserve(buf, 4 + text_max);
		char cut[] = "…\n";
		buf_write(buf, cut, sizeof(cut) - 1);
		buf_write(buf, text.end - text_max, text_max);
	} else
	{
		encode_str(buf, &text);
	}
	encode_obj_end(buf);
	encode_obj_end(buf);

	auto pos = buf->start;
	json_export_as(self->buf, self->timezone, false, 0, &pos);
}

static void
output_jsonrpc_write_none(Output* self)
{
	// result: {}
	output_jsonrpc_begin(self);
	buf_write(self->buf, "{}}", 3);
}

OutputIf output_jsonrpc =
{
	.write       = output_jsonrpc_write,
	.write_str   = output_jsonrpc_write_str,
	.write_data  = output_jsonrpc_write_data,
	.write_error = output_jsonrpc_write_error,
	.write_none  = output_jsonrpc_write_none
};
