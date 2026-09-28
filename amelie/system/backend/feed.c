
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
feed_init(Feed* self, Feeds* feeds, Task* part_task, Part* part)
{
	self->key       = NULL;
	self->ready     = false;
	self->cancel    = false;
	self->part      = part;
	self->part_task = part_task;
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

void
feed_request(Feed* self)
{
	// MSG_FEED
	task_send(self->part_task, &self->msg);
}

void
feed_reply(Feed* self)
{
	// MSG_FEED (to frontend)
	task_send(self->feeds->task, &self->msg);
}

void
feed_cancel(Feed* self)
{
	// MSG_FEED_CANCEL
	task_send(self->part_task, &self->msg_cancel);
}

void
feed_cancel_reply(Feed* self)
{
	// MSG_FEED_CANCEL (to frontend)
	task_send(self->feeds->task, &self->msg_cancel);
}
