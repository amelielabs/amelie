
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
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
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>

hot static inline void
content_json_row_array(Content* self, Columns* columns, Value* row)
{
	auto buf = self->content;
	if (columns->count > 1)
		buf_write(buf, "[", 1);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		value_export(row + column->order, self->local->timezone, self->fmt.opt_pretty, buf);
		content_ensure_limit(self);
	}
	if (columns->count > 1)
		buf_write(buf, "]", 1);
}

hot static inline void
content_json_row_obj(Content* self, Columns* columns, Value* row)
{
	auto buf = self->content;
	if (self->fmt.opt_pretty)
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
				json_export_as(buf, self->local->timezone, true, 1, &pos);
			} else {
				value_export(value, self->local->timezone, true, buf);
			}
			content_ensure_limit(self);
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
		value_export(value, self->local->timezone, false, buf);
		content_ensure_limit(self);
	}
	buf_write(buf, "}", 1);
}

void
content_json(Content* self, Columns* columns, Value* value)
{
	auto buf = self->content;

	// row, ...
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
			if (self->fmt.opt_obj)
				content_json_row_obj(self, columns, row);
			else
				content_json_row_array(self, columns, row);
			store_iterator_next(it);
		}

		// ]
		buf_write(buf, "]", 1);
	} else
	if (value->type == TYPE_JSON)
	{
		// [
		if (! json_is_array(value->json))
			buf_write(buf, "[", 1);

		if (self->fmt.opt_obj)
			content_json_row_obj(self, columns, value);
		else
			content_json_row_array(self, columns, value);

		// ]
		if (! json_is_array(value->json))
			buf_write(buf, "]", 1);
	} else {
		error("operation unsupported");
	}
}
