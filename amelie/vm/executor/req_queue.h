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

typedef struct ReqQueue ReqQueue;

struct ReqQueue
{
	Mutex      lock;
	CondVar    cond_var;
	bool       close;
	List       list;
	int        list_count;
	int        sent;
	atomic_u32 recv;
	atomic_u32 errors;
	atomic_u32 exit;
	Event      event;
};

static inline void
req_queue_init(ReqQueue* self)
{
	self->close      = false;
	self->list_count = 0;
	self->sent       = 0;
	self->recv       = 0;
	self->exit       = 0;
	self->errors     = 0;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
	event_init(&self->event);
}

static inline void
req_queue_free(ReqQueue* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
req_queue_reset(ReqQueue* self)
{
	assert(! self->list_count);
	self->close      = false;
	self->list_count = 0;
	self->sent       = 0;
	self->recv       = 0;
	self->exit       = 0;
	self->errors     = 0;
	list_init(&self->list);
}

static inline void
req_queue_attach(ReqQueue* self)
{
	event_attach(&self->event);
}

static inline void
req_queue_detach(ReqQueue* self)
{
	event_detach(&self->event);
}

static inline void
req_queue_close(ReqQueue* self)
{
	mutex_lock(&self->lock);
	self->close = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
req_queue_send(ReqQueue* self, Req* req, bool close)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &req->link_queue);
	self->list_count++;
	self->sent++;
	if (close)
		self->close = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline int
req_queue_recv(ReqQueue* self)
{
	while (atomic_u32_of(&self->recv) < (uint32_t)self->sent)
		event_wait(&self->event, -1);
	return atomic_u32_of(&self->errors);
}

static inline void
req_queue_recv_exit(ReqQueue* self)
{
	while (! atomic_u32_of(&self->exit))
		event_wait(&self->event, -1);
}

static inline Req*
req_queue_begin(ReqQueue* self)
{
	mutex_lock(&self->lock);
	Req* req = NULL;
	for (;;)
	{
		if (self->list_count > 0)
		{
			auto first = list_pop(&self->list);
			req = container_of(first, Req, link_queue);
			self->list_count--;
		}
		if (self->close)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return req;
}

static inline void
req_queue_end(ReqQueue* self, Req* req)
{
	if (unlikely(req->error))
		atomic_u32_inc(&self->errors);
	atomic_u32_inc(&self->recv);
	event_signal(&self->event);
}

static inline void
req_queue_exit(ReqQueue* self)
{
	atomic_u32_inc(&self->exit);
	event_signal(&self->event);
}
