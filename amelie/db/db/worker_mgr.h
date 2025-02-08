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

typedef struct WorkerMgr WorkerMgr;

struct WorkerMgr
{
	WorkerIf* iface;
	void*     iface_arg;
	HandleMgr mgr;
};

void    worker_mgr_init(WorkerMgr*, WorkerIf*, void*);
void    worker_mgr_free(WorkerMgr*);
void    worker_mgr_create(WorkerMgr*, Tr*, WorkerConfig*, bool);
void    worker_mgr_drop(WorkerMgr*, Tr*, Str*, bool);
void    worker_mgr_dump(WorkerMgr*, Buf*);
Buf*    worker_mgr_list(WorkerMgr*, Str*);
Worker* worker_mgr_find(WorkerMgr*, Str*, bool);
