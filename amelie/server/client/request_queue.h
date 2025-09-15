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

typedef struct RequestQueue RequestQueue;

struct RequestQueue
{
	Spinlock lock;
	List     list;
	Event    on_write;
};

static inline void
request_queue_init(RequestQueue* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	event_init(&self->on_write);
}

static inline void
request_queue_free(RequestQueue* self)
{
	assert(list_empty(&self->list));
	spinlock_free(&self->lock);
}

static inline void
request_queue_attach(RequestQueue* self)
{
	event_attach(&self->on_write);
}

hot static inline void
request_queue_push(RequestQueue* self, Request* req, bool signal)
{
	spinlock_lock(&self->lock);
	list_init(&req->link);
	list_append(&self->list, &req->link);
	spinlock_unlock(&self->lock);
	if (signal)
		event_signal(&self->on_write);
}

static inline Request*
request_queue_peek(RequestQueue* self)
{
	Request* req = NULL;
	spinlock_lock(&self->lock);
	if (! list_empty(&self->list))
		req = container_of(list_pop(&self->list), Request, link);
	spinlock_unlock(&self->lock);
	return req;
}

static inline Request*
request_queue_pop(RequestQueue* self)
{
	assert(self->on_write.bus);
	Request* req;
	while ((req = request_queue_peek(self)) == NULL)
		event_wait(&self->on_write, -1);
	return req;
}
