#pragma once

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

typedef struct Feeds Feeds;

struct Feeds
{
	Feed*   feeds;
	int     feeds_count;
	List    ready;
	Event   notify;
	Iov     iov;
	Client* client;
	Task*   task;
};

void feeds_init(Feeds*, Task*, Client*);
void feeds_free(Feeds*);
void feeds_create(Feeds*, Parts*, Str*);
void feeds_run(Feeds*);

static inline void
feeds_done(Feed* self)
{
	assert(! self->ready);
	self->ready = true;
	list_init(&self->link);

	auto feeds = self->feeds;
	list_append(&feeds->ready, &self->link);
	event_signal(&feeds->notify);
}

static inline void
feeds_cancel(Feed* self)
{
	assert(self->cancel);
	self->cancel = true;
	event_signal(&self->feeds->notify);
}
