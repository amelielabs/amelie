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

typedef struct JobWorker JobWorker;
typedef struct Jobs      Jobs;

struct JobWorker
{
	Jobs* jobs;
	Task  task;
	List  link;
};

JobWorker*
job_worker_allocate(Jobs*);
void job_worker_free(JobWorker*);
void job_worker_start(JobWorker*);
void job_worker_stop(JobWorker*);
