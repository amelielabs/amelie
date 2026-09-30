
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
	if (! heap_iterator_active(it))
	{
		heap_iterator_open(it, part->heap);
	} else
	{
		// reposition to the available next row
		auto row = heap_iterator_at(it);
		if (! row)
			heap_iterator_next(it);
	}

	// collect
	auto data = &self->data;
	for (;;)
	{
		auto row = heap_iterator_at(it);
		if (! row)
			break;
		// todo: if limit
		// todo: write rows to buf
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
