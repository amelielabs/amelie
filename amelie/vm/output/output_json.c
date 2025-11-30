
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

hot static inline void
output_json_row_array(Output* self, Columns* columns, Value* row)
{
	auto buf = self->buf;
	if (columns->count > 1)
		buf_write(buf, "[", 1);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		value_export(row + column->order, self->timezone, self->format_pretty, buf);
		output_ensure_limit(self);
	}
	if (columns->count > 1)
		buf_write(buf, "]", 1);
}

hot static inline void
output_json_row_obj(Output* self, Columns* columns, Value* row)
{
	auto buf = self->buf;
	if (self->format_pretty)
	{
		buf_write(buf, "{\n", 2);
		list_foreach(&columns->list)
		{
			auto column = list_at(Column, link);
			if (column->order > 0)
				buf_write(buf, ",\n", 2);

			// key:
			buf_write(buf, "  \"", 3);
			buf_write_str(buf, &column->name);
			buf_write(buf, "\": ", 3);

			// value
			auto value = row + column->order;
			if (value->type == TYPE_JSON)
			{
				uint8_t* pos = value->json;
				json_export_as(buf, self->timezone, true, 1, &pos);
			} else {
				value_export(value, self->timezone, true, buf);
			}
			output_ensure_limit(self);
		}
		buf_write(buf, "\n}", 2);
		return;
	}

	buf_write(buf, "{", 1);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);

		// key:
		buf_write(buf, "\"", 1);
		buf_write_str(buf, &column->name);
		buf_write(buf, "\": ", 3);

		// value
		auto value = row + column->order;
		value_export(value, self->timezone, false, buf);
		output_ensure_limit(self);
	}
	buf_write(buf, "}", 1);
}

static void
output_json_write(Output* self, Columns* columns, Value* value)
{
	// row, ...
	auto buf = self->buf;
	if (value->type == TYPE_STORE)
	{
		// [
		buf_write(buf, "[", 1);

		auto it = store_iterator(value->store);
		defer(store_iterator_close, it);

		auto first = true;
		Value* row;
		while ((row = store_iterator_at(it)))
		{
			if (! first)
				buf_write(buf, ", ", 2);
			else
				first = false;
			if (self->format_minimal)
				output_json_row_array(self, columns, row);
			else
				output_json_row_obj(self, columns, row);
			store_iterator_next(it);
		}

		// ]
		buf_write(buf, "]", 1);
	} else
	{
		// [
		buf_write(buf, "[", 1);
		if (self->format_minimal)
			output_json_row_array(self, columns, value);
		else
			output_json_row_obj(self, columns, value);
		// ]
		buf_write(buf, "]", 1);
	}
}

static void
output_json_write_json(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	// [
	auto buf  = self->buf;
	auto wrap = true;
	if (unwrap && json_is_array(pos))
		wrap = false;
	if (wrap)
		buf_write(buf, "[", 1);

	if (self->format_minimal)
	{
		json_export_as(buf, self->timezone, self->format_pretty, 0, &pos);
	} else
	{
		// {
		buf_write(buf, "{", 1);
		if (self->format_pretty)
			buf_write(buf, "\n", 1);

		// column: value
		buf_write(buf, "  \"", 3);
		buf_write_str(buf, column);
		buf_write(buf, "\": ", 3);
		json_export_as(buf, self->timezone, self->format_pretty, 1, &pos);

		// }
		if (self->format_pretty)
			buf_write(buf, "\n", 1);
		buf_write(buf, "}", 1);
	}

	// ]
	if (wrap)
		buf_write(buf, "]", 1);
}

static void
output_json_write_error(Output* self, Error* error)
{
	auto buf = buf_create();
	defer_buf(buf);

	Str text;
	buf_str(&error->text, &text);

	// {}
	encode_obj(buf);
	encode_raw(buf, "msg", 3);
	const int text_max = 512;
	if (str_size(&text) > text_max)
	{
		// msg (truncate large error msg)
		encode_string32(buf, 4 + text_max);
		buf_reserve(buf, 4 + text_max);
		buf_write(buf, "...\n", 4);
		buf_write(buf, text.end - text_max, text_max);
	} else
	{
		encode_string(buf, &text);
	}
	encode_obj_end(buf);

	auto pos = buf->start;
	json_export_as(self->buf, self->timezone, self->format_pretty, 0, &pos);
}

OutputIf output_json =
{
	.write       = output_json_write,
	.write_json  = output_json_write_json,
	.write_error = output_json_write_error
};
