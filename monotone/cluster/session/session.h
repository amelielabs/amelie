#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Session Session;

struct Session
{
	ShardMgr* shard_mgr;
	Portal*   portal;
};

void session_init(Session*, ShardMgr*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
