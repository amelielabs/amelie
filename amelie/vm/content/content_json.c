
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>

hot static inline void
content_json_row_array(Content* self, Columns* columns, Value* row)
{
	auto buf = self->content;
	if (columns->list_count > 1)
		buf_write(buf, "[", 1);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column->order > 0)
			buf_write(buf, ", ", 2);
		value_export(row + column->order, self->local->timezone, self->fmt.opt_pretty, buf);
		content_ensure_limit(self);
	}
	if (columns->list_count > 1)
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
			if (value->type == TYPE_JSON && json_is_obj(value->json))
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

	buf_write(buf, "{ ", 2);
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

static inline void
content_json_set(Content* self, Columns* columns, Set* set)
{
	assert(set->count_columns == columns->list_count);

	// {}, ...
	auto buf = self->content;
	if (self->fmt.opt_obj)
	{
		for (auto row = 0; row < set->count_rows; row++)
		{
			if (row > 0)
				buf_write(buf, ", ", 2);
			content_json_row_obj(self, columns, set_row_of(set, row));
		}
		return;
	}

	// [], ...
	for (auto row = 0; row < set->count_rows; row++)
	{
		if (row > 0)
			buf_write(buf, ", ", 2);
		content_json_row_array(self, columns, set_row_of(set, row));
	}
}

static inline void
content_json_merge(Content* self, Columns* columns, Merge* merge)
{
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);

	// {}, ..
	auto buf = self->content;
	if (self->fmt.opt_array)
	{
		auto first = true;
		for (merge_iterator_open(&it, merge); merge_iterator_has(&it);
		     merge_iterator_next(&it))
		{
			auto row = merge_iterator_at(&it);
			if (! first)
				buf_write(buf, ", ", 2);
			else
				first = false;
			content_json_row_obj(self, columns, row);
		}
		return;
	}

	// [], ...
	auto first = true;
	for (merge_iterator_open(&it, merge); merge_iterator_has(&it);
	     merge_iterator_next(&it))
	{
		auto row = merge_iterator_at(&it);
		if (! first)
			buf_write(buf, ", ", 2);
		else
			first = false;
		content_json_row_array(self, columns, row);
	}
}

void
content_json(Content* self, Columns* columns, Value* value)
{
	// [
	auto buf = self->content;
	buf_write(buf, "[", 1);

	// row, ...
	if (value->type == TYPE_SET)
		content_json_set(self, columns, (Set*)value->store);
	else
	if (value->type == TYPE_MERGE)
		content_json_merge(self, columns, (Merge*)value->store);
	else
		error("operation unsupported");

	// ]
	buf_write(buf, "]", 1);
}
