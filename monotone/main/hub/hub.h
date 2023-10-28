#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HubIf HubIf;
typedef struct Hub   Hub;

struct HubIf
{
	void* (*session_create)(Share*, Portal*);
	void  (*session_free)(void*);
	void  (*session_execute)(void*, Buf*);
};

struct Hub
{
	Lock        cat_lock;
	Locker*     cat_locker;
	LockerCache cat_lock_cache;
	ReqCache    req_cache;
	ClientMgr   client_mgr;
	UserCache   user_cache;
	Share       share;
	HubIf*      iface;
	Task        task;
};

void hub_init(Hub*, Share*, HubIf*);
void hub_free(Hub*);
void hub_start(Hub*);
void hub_stop(Hub*);
void hub_add(Hub*, Buf*);
