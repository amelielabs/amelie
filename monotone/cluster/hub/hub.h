#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Hub Hub;

struct Hub
{
	Lock        cat_lock;
	Locker*     cat_locker;
	LockerCache cat_lock_cache;
	ClientMgr   client_mgr;
	UserCache   user_cache;
	Share       share;
	Task        task;
};

void hub_init(Hub*, Share*);
void hub_free(Hub*);
void hub_start(Hub*);
void hub_stop(Hub*);
void hub_add(Hub*, Buf*);
