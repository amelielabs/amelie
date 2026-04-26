
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

hot static inline void
output_json_row_array(Output* self, Columns* columns, Value* row)
{
	auto buf = self->buf;
	buf_write(buf, "[", 1);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		value_export_as(row + column->order, self->timezone, false, 1, buf);
		output_ensure_limit(self);
	}
	buf_write(buf, "]", 1);
}

static void
output_json_write(Output* self, Columns* columns, Value* value)
{
	auto buf = self->buf;
	buf_write(buf, "{", 1);

	// columns
	buf_write(buf, "\"columns\": [", 12);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		buf_write(buf, "\"", 1);
		buf_write_str(buf, &column->name);
		buf_write(buf, "\"", 1);
	}
	buf_write(buf, "], ", 3);

	// rows
	buf_write(buf, "\"rows\": [", 9);
	if (value->type == TYPE_STORE)
	{
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
			output_json_row_array(self, columns, row);
			store_iterator_next(it);
		}
	} else
	{
		output_json_row_array(self, columns, value);
	}
	buf_write(buf, "]}", 2);
}

static void
output_json_write_data(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	auto buf = self->buf;

	buf_write(buf, "{", 1);

	// columns
	buf_write(buf, "\"columns\": [\"", 13);
	buf_write_str(buf, column);
	buf_write(buf, "\"], ", 4);

	// rows
	buf_write(buf, "\"rows\": ", 8);
	auto wrap = true;
	if (unwrap && data_is_array(pos))
		wrap = false;
	if (wrap)
		buf_write(buf, "[", 1);
	json_export_as(buf, self->timezone, false, 1, &pos);
	if (wrap)
		buf_write(buf, "]", 1);
	buf_write(buf, "}", 1);
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

	auto pos = buf->start;
	json_export_as(self->buf, self->timezone, false, 0, &pos);
}

OutputIf output_json =
{
	.write       = output_json_write,
	.write_data  = output_json_write_data,
	.write_error = output_json_write_error,
	.write_none  = NULL
};

void
output_json_result(Output* self, Columns* columns, Value* value)
{
	output_json_write(self, columns, value);
}

void
output_json_result_json(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	output_json_write_data(self, column, pos, unwrap);
}
