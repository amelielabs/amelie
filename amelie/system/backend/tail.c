
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
tail_export(Tail* self, Row* row)
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
tail_init(Tail* self, Tails* tails, Part* part)
{
	self->wait      = false;
	self->shutdown  = false;
	self->ready     = false;
	self->key       = NULL;
	self->part      = part;
	self->part_task = part->track.backend;
	self->part_link = NULL;
	self->tails     = tails;

	msg_init(&self->msg, MSG_TAIL);
	msg_init(&self->msg_cancel, MSG_TAIL_CANCEL);
	event_init(&self->on_complete);
	event_init(&self->on_cancel);
	heap_iterator_init(&self->it);
	buf_init(&self->data);
	list_init(&self->link);
}

void
tail_free(Tail* self)
{
	buf_free(&self->data);
}

hot void
tail_next(Tail* self)
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
			auto timeline = &table_of(part->arg->rel)->timelines.main;
			auto index = part_primary(part);
			auto it = index_iterator(index);
			defer(iterator_close, it);
			auto match = iterator_open(it, part->heap, timeline, self->key);
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

		tail_export(self, row);
		// todo: if limit
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
	self->part_link = part->tails;
	part->tails     = self;
}

void
tail_cancel(Tail* self)
{
	// unlink tail from the wait list
	if (self->wait)
	{
		auto tail = (Tail*)self->part->tails;
		if (tail == self)
		{
			self->part->tails = self->part_link;
		} else
		{
			for (; tail; tail = tail->part_link)
			{
				if (tail->part_link == self)
				{
					tail->part_link = self->part_link;
					break;
				}
			}
		}
		self->part_link = NULL;
		self->wait = false;

		// notify tail completion
		event_signal(&self->on_complete);
	}

	// notify cancel completion
	event_signal(&self->on_cancel);
}

void
tail_cancel_all(Part* self)
{
	// cancel waiters
	auto tail = (Tail*)self->tails;
	self->tails = NULL;
	while (tail)
	{
		auto next = tail->part_link;
		tail->part_link = NULL;
		tail->wait      = false;
		tail->shutdown  = true;

		// notify completion
		event_signal(&tail->on_complete);
		tail = next;
	}
}

hot void
tail_resume_all(Part* self)
{
	auto tail = (Tail*)self->tails;
	self->tails = NULL;
	while (tail)
	{
		auto next = tail->part_link;
		tail->part_link = NULL;
		tail_next(tail);
		tail = next;
	}
}
