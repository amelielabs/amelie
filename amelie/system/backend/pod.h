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

typedef struct Pod Pod;

struct Pod
{
	Vm      vm;
	Track*  track;
	Part*   part;
	int64_t worker_id;
	List    link;
};

Pod* pod_allocate(Part*);
void pod_free(Pod*);
void pod_start(Pod*, Task*);
void pod_stop(Pod*);
