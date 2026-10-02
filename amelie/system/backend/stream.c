
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
#include <amelie_backend.h>

hot static void
stream_export(Stream* self, Row* row)
{
	Value value;
	value_init(&value);

	auto buf = &self->data;
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

void
stream_init(Stream* self, Streams* streams, Part* part)
{
	self->wait      = false;
	self->shutdown  = false;
	self->ready     = false;
	self->key       = NULL;
	self->part      = part;
	self->part_task = part->track.backend;
	self->part_link = NULL;
	self->streams     = streams;

	auto timeline = &self->timeline;
	timeline_init(timeline);
	timeline->main = true;

	msg_init(&self->msg, MSG_STREAM);
	msg_init(&self->msg_cancel, MSG_STREAM_CANCEL);
	event_init(&self->on_complete);
	event_init(&self->on_cancel);
	heap_iterator_init(&self->it);
	buf_init(&self->data);
	list_init(&self->link);
}

void
stream_free(Stream* self)
{
	buf_free(&self->data);
}

hot void
stream_next(Stream* self)
{
	// todo: validate partition
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

		// set heap position using the primary index key
		if (self->key)
		{
			auto index = part_primary(part);
			auto it = index_iterator(index);
			defer(iterator_close, it);
			auto match = iterator_open(it, part->heap, &self->timeline, self->key);
			if (match)
				iterator_next(it);
			auto row = it->current;
			if (row)
				heap_iterator_set(&self->it, row);
		}
	}

	// collect
	auto data = &self->data;
	for (;;)
	{
		auto row = heap_iterator_at(it);
		if (!row || !row->commited)
			break;
		if (! row->deleted)
			stream_export(self, row);
		// todo: limit
		heap_iterator_next(it);
	}

	if (! buf_empty(data))
	{
		// notify completion
		event_signal(&self->on_complete);
		return;
	}

	// add to the wait list
	self->wait      = true;
	self->part_link = part->streams;
	part->streams     = self;
}

void
stream_cancel(Stream* self)
{
	// unlink stream from the wait list
	if (self->wait)
	{
		auto stream = (Stream*)self->part->streams;
		if (stream == self)
		{
			self->part->streams = self->part_link;
		} else
		{
			for (; stream; stream = stream->part_link)
			{
				if (stream->part_link == self)
				{
					stream->part_link = self->part_link;
					break;
				}
			}
		}
		self->part_link = NULL;
		self->wait = false;

		// notify stream completion
		event_signal(&self->on_complete);
	}

	// notify cancel completion
	event_signal(&self->on_cancel);
}

void
streaming_cancel(Part* self)
{
	// cancel waiters
	auto stream = (Stream*)self->streams;
	self->streams = NULL;
	while (stream)
	{
		auto next = stream->part_link;
		stream->part_link = NULL;
		stream->wait      = false;
		stream->shutdown  = true;

		// notify completion
		event_signal(&stream->on_complete);
		stream = next;
	}
}

hot void
streaming_resume(Part* self)
{
	auto stream = (Stream*)self->streams;
	self->streams = NULL;
	while (stream)
	{
		auto next = stream->part_link;
		stream->part_link = NULL;
		stream_next(stream);
		stream = next;
	}
}
