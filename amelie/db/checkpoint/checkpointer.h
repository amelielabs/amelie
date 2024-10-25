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

typedef struct Checkpointer Checkpointer;

struct Checkpointer
{
	RpcQueue       req_queue;
	Event          req_queue_event;
	int64_t        worker_id;
	int64_t        worker_periodic_id;
	int            interval_us;
	CheckpointMgr* mgr;
};

void checkpointer_init(Checkpointer*, CheckpointMgr*);
void checkpointer_start(Checkpointer*);
void checkpointer_stop(Checkpointer*);
void checkpointer_request(Checkpointer*, Rpc*);
