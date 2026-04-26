
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

static void
output_jsonrpc_write(Output* self, Columns* columns, Value* value)
{
	char header[] = "{\"jsonrpc\": \"2.0\", \"id\": 0, \"result\": ";
	auto buf = self->buf;
	buf_write(buf, header, sizeof(header) - 1);
	output_json_result(self, columns, value);
	buf_write(buf, "}", 1);
}

static void
output_jsonrpc_write_data(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	char header[] = "{\"jsonrpc\": \"2.0\", \"id\": 0, \"result\": ";
	auto buf = self->buf;
	buf_write(buf, header, sizeof(header) - 1);
	output_json_result_json(self, column, pos, unwrap);
	buf_write(buf, "}", 1);
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
	encode_int(buf, 0);

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
	auto buf = self->buf;
	char header[] = "{\"jsonrpc\": \"2.0\", \"id\": 0, \"result\": {}}";
	buf_write(buf, header, sizeof(header) - 1);
}

OutputIf output_jsonrpc =
{
	.write       = output_jsonrpc_write,
	.write_data  = output_jsonrpc_write_data,
	.write_error = output_jsonrpc_write_error,
	.write_none  = output_jsonrpc_write_none
};
