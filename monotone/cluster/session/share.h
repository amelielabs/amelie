#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Share Share;

struct Share
{
	TableMgr*     table_mgr;
	MetaMgr*      meta_mgr;
	ShardMgr*     shard_mgr;
	RequestSched* req_sched;
};
