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

typedef struct JobList JobList;

struct JobList
{
	int  list_count;
	List list;
};

static inline void
job_list_init(JobList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

hot static inline Job*
job_list_pop(JobList* self)
{
	if (self->list_count == 0)
		return NULL;
	auto first = list_pop(&self->list);
	auto job = container_of(first, Job, link);
	self->list_count--;
	return job;
}

static inline void
job_list_add(JobList* self, Job* job)
{
	list_append(&self->list, &job->link);
	self->list_count++;
}

static inline void
job_list_del(JobList* self, Job* job)
{
	list_unlink(&job->link);
	self->list_count--;
}
