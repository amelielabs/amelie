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

typedef struct Service Service;

struct Service
{
	Mutex   lock;
	CondVar cond_var;
	List    list;
	int     list_count;
	bool    shutdown;
};

static inline void
service_init(Service* self)
{
	self->shutdown   = false;
	self->list_count = 0;
	list_init(&self->list);
	cond_var_init(&self->cond_var);
	mutex_init(&self->lock);
}

static inline void
service_free(Service* self)
{
	list_foreach_safe(&self->list)
	{
		auto req = list_at(ServiceReq, link);
		service_req_free(req);
	}
	cond_var_free(&self->cond_var);
	mutex_free(&self->lock);
}

static inline void
service_shutdown(Service* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_broadcast(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
service_schedule(Service* self, ServiceReq* req)
{
	mutex_lock(&self->lock);
	list_append(&self->list, &req->link);
	self->list_count++;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline void
service_add(Service* self, uint64_t id, int count, ...)
{
	// allocate request
	assert(count > 0);
	va_list args;
	va_start(args, count);
	auto req = service_req_allocate(id, count, args);
	va_end(args);

	// schedule request execution based on the
	// first action type
	service_schedule(self, req);
}

static inline void
service_gc(Service* self)
{
	service_add(self, UINT64_MAX, 1,
	            ACTION_GC);
}

static inline void
service_rotate(Service* self)
{
	service_add(self, UINT64_MAX, 2,
	            ACTION_ROTATE,
	            ACTION_GC);
}

static inline void
service_refresh(Service* self, Part* part)
{
	service_add(self, part->config->id, 3,
	            ACTION_REFRESH,
	            ACTION_GC);
}

static inline bool
service_next(Service* self, bool wait, ServiceReq** req)
{
	mutex_lock(&self->lock);

	auto shutdown = false;
	for (;;)
	{
		if (unlikely(self->shutdown))
		{
			shutdown = true;
			break;
		}

		if (self->list_count > 0)
		{
			*req = container_of(list_pop(&self->list), ServiceReq, link);
			self->list_count--;
			break;
		}
		if (! wait)
			break;

		cond_var_wait(&self->cond_var, &self->lock);
	}

	mutex_unlock(&self->lock);
	return shutdown;
}
