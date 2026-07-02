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

typedef struct Jobs Jobs;

struct Jobs
{
	Mutex   lock;
	CondVar cond_var;
	bool    shutdown;
	List    jobs;
	int     jobs_count;
	List    workers;
	int     workers_count;
};

void jobs_init(Jobs*);
void jobs_free(Jobs*);
void jobs_start(Jobs*, int);
void jobs_stop(Jobs*);
void jobs_add(Jobs*, Job*);
Job* jobs_next(Jobs*);
