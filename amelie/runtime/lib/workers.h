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

typedef struct Workers Workers;

struct Workers
{
	Mutex   lock;
	CondVar cond_var;
	bool    shutdown;
	List    reqs;
	int     reqs_count;
	List    workers;
	int     workers_count;
};

void workers_init(Workers*);
void workers_free(Workers*);
void workers_start(Workers*, int);
void workers_stop(Workers*);
void workers_add(Workers*, WorkerReq*);
WorkerReq*
workers_next(Workers*);
