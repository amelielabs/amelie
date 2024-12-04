
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
body_ensure_limit(Body* self)
{
	auto limit = var_int_of(&config()->limit_send);
	if (unlikely((uint64_t)buf_size(self->buf) >= limit))
		error("reply limit reached");
}

hot static inline void
body_write_set_obj(Body* self, Columns* columns, Set* set)
{
	assert(set->count_columns == columns->list_count);

	auto buf = self->buf;
	for (auto row = 0; row < set->count_rows; row++)
	{
		if (row > 0)
			buf_write(buf, ", ", 2);

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
			auto value = set_column_of(set, row, column->order);
			if (value->type == TYPE_JSON && json_is_obj(value->json))
			{
				uint8_t* pos = value->json;
				json_export_as(buf, self->local->timezone, true, 1, &pos);
			} else {
				value_export(value, self->local->timezone, true, buf);
			}
			body_ensure_limit(self);
		}

		buf_write(buf, "\n}", 2);
	}
}

hot static inline void
body_write_set_array(Body* self, Columns* columns, Set* set)
{
	(void)columns;
	assert(set->count_columns == columns->list_count);

	auto buf = self->buf;
	for (auto row = 0; row < set->count_rows; row++)
	{
		if (row > 0)
			buf_write(buf, ", ", 2);
		if (set->count_columns > 1)
			buf_write(buf, "[", 1);
		for (auto col = 0; col < set->count_columns; col++)
		{
			if (col > 0)
				buf_write(buf, ", ", 2);
			value_export(set_column_of(set, row, col),
			             self->local->timezone,
			             true, buf);
			body_ensure_limit(self);
		}
		if (set->count_columns > 1)
			buf_write(buf, "]", 1);
	}
}

hot static inline void
body_write_merge_obj(Body* self, Columns* columns, Merge* merge)
{
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);
	auto first = true;
	auto buf = self->buf;
	for (merge_iterator_open(&it, merge);
	     merge_iterator_has(&it);
	     merge_iterator_next(&it))
	{
		auto row = merge_iterator_at(&it);
		if (! first)
			buf_write(buf, ", ", 2);
		else
			first = false;
		auto set = it.current_it->set;
		assert(set->count_columns == columns->list_count);

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

			body_ensure_limit(self);
		}
		buf_write(buf, "\n}", 2);
	}
}

hot static inline void
body_write_merge_array(Body* self, Columns* columns, Merge* merge)
{
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);
	auto first = true;
	auto buf = self->buf;
	for (merge_iterator_open(&it, merge);
	     merge_iterator_has(&it);
	     merge_iterator_next(&it))
	{
		auto row = merge_iterator_at(&it);
		if (! first)
			buf_write(buf, ", ", 2);
		else
			first = false;
		auto set = it.current_it->set;
		assert(set->count_columns == columns->list_count);

		if (set->count_columns > 1)
			buf_write(buf, "[", 1);
		for (auto col = 0; col < set->count_columns; col++)
		{
			if (col > 0)
				buf_write(buf, ", ", 2);
			value_export(row + col, self->local->timezone, true, buf);
			body_ensure_limit(self);
		}
		if (set->count_columns > 1)
			buf_write(buf, "]", 1);
	}
}

void
body_init(Body* self, Local* local, Buf* buf)
{
	self->buf   = buf;
	self->local = local;
}

void
body_reset(Body* self)
{
	buf_reset(self->buf);
}

hot void
body_write(Body* self, Columns* columns, Value* value)
{
	// [
	buf_write(self->buf, "[", 1);

	(void)body_write_set_obj;
	(void)body_write_merge_obj;

	// {}, ...
	if (value->type == TYPE_SET)
		body_write_set_array(self, columns, (Set*)value->store);
	else
	if (value->type == TYPE_MERGE)
		body_write_merge_array(self, columns, (Merge*)value->store);
	else
		error("operation unsupported");

	// ]
	buf_write(self->buf, "]", 1);
}

void
body_write_json(Body* self, Buf* buf, bool wrap)
{
	// wrap body in [] unless returning data is array
	
	// [
	uint8_t* pos = buf->start;
	wrap = wrap && buf_empty(self->buf) && !json_is_array(pos);
	if (wrap)
		buf_write(self->buf, "[", 1);

	// {}
	json_export_pretty(self->buf, self->local->timezone, &pos);

	// ]
	if (wrap)
		buf_write(self->buf, "]", 1);
	body_ensure_limit(self);
}

void
body_write_error(Body* self, Error* error)
{
	// {}
	auto buf = buf_create();
	guard_buf(buf);
	encode_obj(buf);
	encode_raw(buf, "msg", 3);
	encode_raw(buf, error->text, error->text_len);
	encode_obj_end(buf);

	uint8_t* pos = buf->start;
	json_export(self->buf, NULL, &pos);
}
