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
typedef struct Dtr      Dtr;

enum
{
	JOB_EXECUTE,
	JOB_PREPARE,
	JOB_COMMIT,
	JOB_ABORT
};

struct Job
{
	int       type;
	int       start;
	Buf       arg;
	Value     result;
	Tr*       tr;
	Dtr*      dtr;
	Buf*      error;
	JobCache* cache;
	Queue*    queue;
	QueueNode queue_node;
	List      link_executor;
	List      link;
};

static inline Job*
job_allocate(void)
{
	auto self = (Job*)am_malloc(sizeof(Job));
	self->type  = 0;
	self->start = 0;
	self->tr    = NULL;
	self->dtr   = NULL;
	self->error = NULL;
	self->cache = NULL;
	self->queue = NULL;
	buf_init(&self->arg);
	value_init(&self->result);
	queue_node_init(&self->queue_node);
	list_init(&self->link_executor);
	list_init(&self->link);
	return self;
}

static inline void
job_free_memory(Job* self)
{
	value_free(&self->result);
	buf_free(&self->arg);
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
	self->type  = 0;
	self->start = 0;
	self->tr    = NULL;
	self->dtr   = NULL;
	self->cache = NULL;
	self->queue = NULL;
	value_free(&self->result);
	buf_reset(&self->arg);
	queue_node_reset(&self->queue_node);
}

always_inline static inline Job*
job_of(QueueNode* node)
{
	return container_of(node, Job, queue_node);
}
