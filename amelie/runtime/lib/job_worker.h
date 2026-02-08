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
typedef struct JobMgr    JobMgr;

struct JobWorker
{
	JobMgr* job_mgr;
	Task    task;
	List    link;
};

JobWorker*
job_worker_allocate(JobMgr*);
void job_worker_free(JobWorker*);
void job_worker_start(JobWorker*);
void job_worker_stop(JobWorker*);
