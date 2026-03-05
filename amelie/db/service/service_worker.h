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

typedef struct ServiceWorker ServiceWorker;

struct ServiceWorker
{
	int64_t  coroutine_id;
	Service* service;
	List     link;
};

static inline ServiceWorker*
service_worker_allocate(Service* service)
{
	auto self = (ServiceWorker*)am_malloc(sizeof(ServiceWorker));
	self->coroutine_id = -1;
	self->service      = service;
	list_init(&self->link);
	return self;
}

static inline void
service_worker_free(ServiceWorker* self)
{
	am_free(self);
}
