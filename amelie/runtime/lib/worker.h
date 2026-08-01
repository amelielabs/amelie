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

typedef struct Worker  Worker;
typedef struct Workers Workers;

struct Worker
{
	Workers* workers;
	Task     task;
	List     link;
};

Worker*
worker_allocate(Workers*);
void worker_free(Worker*);
void worker_start(Worker*);
void worker_stop(Worker*);
