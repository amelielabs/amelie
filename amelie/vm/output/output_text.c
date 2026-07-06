
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
output_text_write(Output* self, Columns* columns, Value* value)
{
	auto print = &self->print;
	print_create(print, columns, value, self->timezone, self->buf);
	print_run(print);
}

static void
output_text_write_str(Output* self, Str* column, Str* str)
{
	auto buf = self->buf;
	buf_write(buf, "\n", 1);

	// column
	auto column_size = str_size(column);

	buf_write_str(buf, column);
	buf_write(buf, "\n", 1);
	for (auto i = 0; i < column_size; i++)
		buf_write(buf, "─", sizeof("─") - 1);
	buf_write(buf, "\n", 1);

	// value
	unescape_str(buf, str);
}

static void
output_text_write_data(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	unused(unwrap);
	auto buf = self->buf;
	buf_write(buf, "\n", 1);

	// column
	auto column_size = str_size(column);

	buf_write_str(buf, column);
	buf_write(buf, "\n", 1);
	for (auto i = 0; i < column_size; i++)
		buf_write(buf, "─", sizeof("─") - 1);
	buf_write(buf, "\n", 1);

	if (data_is_array(pos))
	{
		unpack_array(&pos);
		while (! unpack_array_end(&pos))
		{
			json_export_as(self->buf, self->timezone, true, 0, &pos);
			buf_write(buf, "\n", 1);
		}
	} else
	{
		// value
		json_export_as(self->buf, self->timezone, true, 0, &pos);
		buf_write(buf, "\n", 1);
	}
}

static void
output_text_write_error(Output* self, Error* error)
{
	auto buf = self->buf;
	buf_write(buf, "error: ", 7);

	Str text;
	buf_str(&error->text, &text);

	const int text_max = 512;
	if (str_size(&text) > text_max)
	{
		// truncate large error msg
		char cut[] = "…\n";
		buf_write(buf, cut, sizeof(cut) - 1);
		buf_write(buf, text.end - text_max, text_max);
	} else
	{
		buf_write_str(buf, &text);
	}
	buf_write(buf, "\n", 1);
}

OutputIf output_text =
{
	.write       = output_text_write,
	.write_str   = output_text_write_str,
	.write_data  = output_text_write_data,
	.write_error = output_text_write_error,
	.write_none  = NULL
};
