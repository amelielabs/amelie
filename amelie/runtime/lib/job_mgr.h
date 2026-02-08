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

typedef struct JobMgr JobMgr;

struct JobMgr
{
	Mutex   lock;
	CondVar cond_var;
	bool    shutdown;
	List    jobs;
	int     jobs_count;
	List    workers;
	int     workers_count;
};

void job_mgr_init(JobMgr*);
void job_mgr_free(JobMgr*);
void job_mgr_start(JobMgr*, int);
void job_mgr_stop(JobMgr*);
void job_mgr_add(JobMgr*, Job*);
Job* job_mgr_next(JobMgr*);
