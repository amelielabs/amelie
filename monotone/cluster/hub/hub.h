#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Hub Hub;

struct Hub
{
	ShardMgr*     shard_mgr;
	RequestSched* req_sched;
	ClientMgr     client_mgr;
	UserCache     user_cache;
	Task          task;
};

void hub_init(Hub*, ShardMgr*, RequestSched*);
void hub_free(Hub*);
void hub_start(Hub*);
void hub_stop(Hub*);
void hub_add(Hub*, Buf*);
