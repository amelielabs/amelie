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
	Mutex   lock;
	CondVar cond_var;
	bool    close;
	List    list;
	int     list_count;
};

static inline void
req_queue_init(ReqQueue* self)
{
	self->close      = false;
	self->list_count = 0;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->list);
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
	list_init(&self->list);
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
	if (close)
		self->close = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline Req*
req_queue_pop(ReqQueue* self)
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
			break;
		}
		if (self->close)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return req;
}
