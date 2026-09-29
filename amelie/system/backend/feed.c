
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
feed_init(Feed* self, Feeds* feeds, Task* task, Task* part_task, Part* part)
{
	self->key       = NULL;
	self->ready     = false;
	self->cancel    = false;
	self->wait      = false;
	self->error     = false;
	self->part      = part;
	self->part_task = part_task;
	self->part_link = NULL;
	self->task      = task;
	self->feeds     = feeds;

	msg_init(&self->msg, MSG_FEED);
	msg_init(&self->msg_cancel, MSG_FEED_CANCEL);
	heap_iterator_init(&self->it);
	buf_init(&self->data);
	list_init(&self->link);
}

void
feed_free(Feed* self)
{
	buf_free(&self->data);
}

hot void
feed_next(Feed* self)
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
		// MSG_FEED (to frontend)
		task_send(self->task, &self->msg);
		return;
	}

	// add to the wait list
	self->wait      = true;
	self->part_link = part->feeds;
	part->feeds     = self;
}

void
feed_cancel(Feed* self)
{
	// unlink feed from wait list
	if (self->wait)
	{
		self->wait = false;
		auto feed = (Feed*)self->part->feeds;
		if (feed == self)
		{
			self->part->feeds = self->part_link;
		} else
		{
			for (; feed; feed = feed->part_link)
			{
				if (feed->part_link == self)
				{
					feed->part_link = self->part_link;
					break;
				}
			}
		}
		self->part_link = NULL;
	}

	// MSG_FEED_CANCEL (to frontend)
	task_send(self->task, &self->msg_cancel);
}

void
feed_cancel_all(Part* self)
{
	// cancel waiters
	auto feed = (Feed*)self->feeds;
	self->feeds = NULL;
	while (feed)
	{
		auto next = feed->part_link;
		feed->part_link = NULL;
		feed->wait      = false;
		feed->error     = true;

		// MSG_FEED (to frontend)
		task_send(feed->task, &feed->msg);
		feed = next;
	}
}

hot void
feed_resume(Part* self)
{
	auto feed = (Feed*)self->feeds;
	self->feeds = NULL;
	while (feed)
	{
		auto next = feed->part_link;
		feed->part_link = NULL;
		feed_next(feed);
		feed = next;
	}
}
