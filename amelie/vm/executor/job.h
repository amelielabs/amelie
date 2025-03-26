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
typedef struct JobArg   JobArg;
typedef struct JobCache JobCache;
typedef struct Dtr      Dtr;

enum
{
	JOB_EXECUTE,
	JOB_COMMIT,
	JOB_ABORT
};

struct JobArg
{
	int   type;
	int   step;
	Buf   arg;
	Value result;
	Buf*  error;
	Dtr*  dtr;
	Tr*   tr;
	int   start;
	Part* part;
};

struct Job
{
	uint64_t      id;
	JobArg        arg;
	JobCache*     cache;
	HashtableNode node;
	Job*          queue;
	Job*          queue_tail;
	Job*          next;
	List          link_mgr;
	List          link;
};

static inline void
job_arg_init(JobArg* self)
{
	self->type  = 0;
	self->step  = 0;
	self->error = NULL;
	self->dtr   = NULL;
	self->tr    = NULL;
	self->start = 0;
	self->part  = NULL;
	buf_init(&self->arg);
	value_init(&self->result);
}

static inline void
job_arg_free(JobArg* self)
{
	if (self->error)
		buf_free(self->error);
	buf_free(&self->arg);
	value_free(&self->result);
}

static inline void
job_arg_reset(JobArg* self)
{
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->type  = 0;
	self->step  = 0;
	self->dtr   = NULL;
	self->tr    = NULL;
	self->start = 0;
	self->part  = NULL;
	buf_reset(&self->arg);
	value_free(&self->result);
}

static inline Job*
job_allocate(void)
{
	auto self = (Job*)am_malloc(sizeof(Job));
	self->id         = UINT64_MAX;
	self->cache      = NULL;
	self->queue      = NULL;
	self->queue_tail = NULL;
	self->next       = NULL;
	job_arg_init(&self->arg);
	hashtable_node_init(&self->node);
	list_init(&self->link_mgr);
	list_init(&self->link);
	return self;
}

static inline void
job_free_memory(Job* self)
{
	job_arg_free(&self->arg);
	am_free(self);
}

static inline void
job_reset(Job* self)
{
	self->queue      = NULL;
	self->queue_tail = NULL;
	self->next       = NULL;
	job_arg_reset(&self->arg);
}
