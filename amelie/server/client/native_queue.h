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

typedef struct NativeQueue NativeQueue;

struct NativeQueue
{
	Spinlock lock;
	List     list;
	Event    on_write;
};

static inline void
native_queue_init(NativeQueue* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
	event_init(&self->on_write);
}

static inline void
native_queue_free(NativeQueue* self)
{
	assert(list_empty(&self->list));
	spinlock_free(&self->lock);
}

static inline void
native_queue_attach(NativeQueue* self)
{
	event_attach(&self->on_write);
}

hot static inline void
native_queue_push(NativeQueue* self, NativeReq* req, bool signal)
{
	spinlock_lock(&self->lock);
	list_init(&req->link);
	list_append(&self->list, &req->link);
	spinlock_unlock(&self->lock);
	if (signal)
		event_signal(&self->on_write);
}

static inline NativeReq*
native_queue_peek(NativeQueue* self)
{
	NativeReq* req = NULL;
	spinlock_lock(&self->lock);
	if (! list_empty(&self->list))
		req = container_of(list_pop(&self->list), NativeReq, link);
	spinlock_unlock(&self->lock);
	return req;
}

static inline NativeReq*
native_queue_pop(NativeQueue* self)
{
	assert(self->on_write.bus);
	NativeReq* req;
	while ((req = native_queue_peek(self)) == NULL)
		event_wait(&self->on_write, -1);
	return req;
}
