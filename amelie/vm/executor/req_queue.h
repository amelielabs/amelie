#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ReqQueue ReqQueue;

struct ReqQueue
{
	Mutex   lock;
	CondVar cond_var;
	int     list_count;
	List    list;
};

static inline void
req_queue_init(ReqQueue* self)
{
	self->list_count = 0;
	list_init(&self->list);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
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
	mutex_lock(&self->lock);
	self->list_count = 0;
	list_init(&self->list);
	mutex_unlock(&self->lock);
}

static inline Req*
req_queue_pop(ReqQueue* self)
{
	mutex_lock(&self->lock);
	for (;;)
	{
		if (self->list_count > 0)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	auto first = list_pop(&self->list);
	auto req = container_of(first, Req, link_queue);
	self->list_count--;
	mutex_unlock(&self->lock);
	return req;
}

static inline void
req_queue_add(ReqQueue* self, Req* req)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &req->link_queue);
	self->list_count++;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}
