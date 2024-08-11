#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Checkpointer Checkpointer;

struct Checkpointer
{
	RpcQueue       req_queue;
	Event          req_queue_event;
	int64_t        coroutine_id;
	CheckpointMgr* mgr;
};

void checkpointer_init(Checkpointer*, CheckpointMgr*);
void checkpointer_start(Checkpointer*);
void checkpointer_stop(Checkpointer*);
void checkpointer_request(Checkpointer*, Rpc*);
