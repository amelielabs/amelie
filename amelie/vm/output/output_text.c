
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>

static void
output_text_write(Output* self, Columns* columns, Value* value)
{
	auto buf = self->buf;
	if (value->type == TYPE_STORE)
	{
		auto it = store_iterator(value->store);
		defer(store_iterator_close, it);

		auto first = true;
		Value* row;
		while ((row = store_iterator_at(it)))
		{
			if (! first)
				buf_write(buf, "\n", 1);
			else
				first = false;

			list_foreach(&columns->list)
			{
				auto column = list_at(Column, link);
				if (column->order > 0)
					buf_write(buf, ", ", 2);
				value_export(row + column->order, self->timezone, self->format_pretty, buf);
				output_ensure_limit(self);
			}

			store_iterator_next(it);
		}
		return;
	}

	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		value_export(value + column->order, self->timezone, self->format_pretty, buf);
		output_ensure_limit(self);
	}
}

static void
output_text_write_json(Output* self, Str* column, uint8_t* pos, bool unwrap)
{
	unused(column);
	unused(unwrap);
	json_export_as(self->buf, self->timezone, self->format_pretty, 0, &pos);
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
}

OutputIf output_text =
{
	.write       = output_text_write,
	.write_json  = output_text_write_json,
	.write_error = output_text_write_error
};
