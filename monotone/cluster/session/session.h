#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Session Session;

struct Session
{
	RequestSet    req_set;
	RequestCache  req_cache;
	RequestSched* req_sched;
	ShardMgr*     shard_mgr;
	Portal*       portal;
};

void session_init(Session*, ShardMgr*, RequestSched*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
