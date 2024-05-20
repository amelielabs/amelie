#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct ReqQueue ReqQueue;

struct ReqQueue
{
	Mutex   lock;
	CondVar cond_var;
	int     list_count;
	List    list;
	bool    shutdown;
};

static inline void
req_queue_init(ReqQueue* self)
{
	self->shutdown   = false;
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
	self->shutdown   = false;
	self->list_count = 0;
	list_init(&self->list);
	mutex_unlock(&self->lock);
}

static inline Req*
req_queue_get(ReqQueue* self)
{
	mutex_lock(&self->lock);
	for (;;)
	{
		if (self->list_count > 0)
			break;
		if (self->shutdown)
		{
			mutex_unlock(&self->lock);
			return NULL;
		}
		cond_var_wait(&self->cond_var, &self->lock);
	}
	auto first = list_pop(&self->list);
	auto req = container_of(first, Req, link_queue);
	self->list_count--;
	mutex_unlock(&self->lock);
	return req;
}

static inline void
req_queue_add(ReqQueue* self, Req* req, bool shutdown)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &req->link_queue);
	self->list_count++;
	self->shutdown = shutdown;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
req_queue_shutdown(ReqQueue* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}
