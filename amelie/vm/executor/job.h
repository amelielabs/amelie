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

typedef struct Job      Job;
typedef struct JobCache JobCache;
typedef struct Dispatch Dispatch;
typedef struct Queue    Queue;

struct Job
{
	int       type;
	uint64_t  id;
	int       id_step;
	bool      last;
	Buf       data;
	Buf*      error;
	Dispatch* dispatch;
	Queue*    queue;
	JobCache* cache;
	List      link_queue;
	List      link_scheduler;
	List      link;
};

static inline Job*
job_allocate(void)
{
	auto self = (Job*)am_malloc(sizeof(Job));
	self->type     = 0;
	self->id       = 0;
	self->id_step  = 0;
	self->last     = false;
	self->error    = NULL;
	self->dispatch = NULL;
	self->queue    = NULL;
	self->cache    = NULL;
	buf_init(&self->data);
	list_init(&self->link_queue);
	list_init(&self->link_scheduler);
	list_init(&self->link);
	return self;
}

static inline void
job_free_memory(Job* self)
{
	if (self->error)
		buf_free(self->error);
	am_free(self);
}

static inline void
job_reset(Job* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->type     = 0;
	self->id       = 0;
	self->id_step  = 0;
	self->dispatch = NULL;
	self->queue    = NULL;
	buf_reset(&self->data);
}
