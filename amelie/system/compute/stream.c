
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
#include <amelie_vm>
#include <amelie_compute.h>

void
stream_init(Stream* self, StreamPos* pos, Part* part)
{
	self->pos  = *pos;
	self->part = part;
	heap_iterator_init(&self->it);
	sub_init(&self->sub);
	buf_init(&self->data);
}

void
stream_free(Stream* self)
{
	buf_free(&self->data);
}

hot static void
stream_export(Stream* self, Row* row)
{
	Value value;
	value_init(&value);

	uint64_t id = heap_page_of(row)->id * (2 * 1024 * 1024) + row->offset;
	auto buf = &self->data;
	buf_format(buf, "id: {u64}\n", id);
	buf_write(buf, "data: ", 6);
	buf_write(buf, "{", 1);

	auto list = &self->part->arg->columns->list;
	list_foreach(list)
	{
		auto column = list_at(Column, link);
		buf_format(buf, "{qstr}: ", &column->name);

		auto data = row_column(row, column);
		if (! data)
		{
			value_set_null(&value);
		} else
		if (column->type == TYPE_VECTOR)
		{
			auto flat = flats_at(&self->part->flats, column);
			auto vector = (float*)flat_vector_at(flat, *(uint32_t*)data);
			value_set_vector(&value, column->size_flat / sizeof(float), vector, NULL);
		} else
		{
			auto size = column->size;
			if (! size)
			{
				uint8_t* start = data;
				uint8_t* pos = start;
				data_skip(&pos);
				size = pos - start;
			}
			value_data_decode(&value, column, data, size);
		}
		value_export(&value, runtime()->timezone, false, buf);

		if (! list_is_first(list, &column->link))
			buf_write(buf, ", ", 2);
	}
	buf_write(buf, "}\n\n", 3);
}

hot int
stream_next(Stream* self)
{
	auto part = self->part;
	auto it = &self->it;
	if (likely(heap_iterator_active(it)))
	{
		// reposition to the available next row
		auto row = heap_iterator_at(it);
		if (! row)
			heap_iterator_next(it);
	} else
	{
		// on first access, position to last
		heap_iterator_open(it, part->heap, true);

		// open by position
		auto pos = &self->pos;
		if (! stream_pos_empty(pos))
		{
			if (! heap_iterator_set(it, pos->page, pos->page_offset))
				return -1;
			heap_iterator_next(it);
		}
	}

	// collect
	auto data = &self->data;
	buf_reset(data);
	for (;;)
	{
		auto row = heap_iterator_at(it);
		if (!row || !row->commited)
			break;
		stream_export(self, row);
		if (buf_size(data) >= 128 * 1024)
			break;
		heap_iterator_next(it);
	}

	// has data
	return !buf_empty(data);
}
