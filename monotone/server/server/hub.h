#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HubIf HubIf;
typedef struct Hub   Hub;

typedef void (*HubMain)(void*, Client*);
typedef void (*HubMainNative)(void*, Native*);

struct HubIf
{
	HubMain       main[ACCESS_MAX];
	HubMainNative main_native;
};

struct Hub
{
	ClientMgr client_mgr;
	UserCache user_cache;
	HubIf*    iface;
	void*     iface_arg;
	Task      task;
};

void hub_init(Hub*, HubIf*, void*);
void hub_free(Hub*);
void hub_start(Hub*);
void hub_stop(Hub*);
void hub_add(Hub*, Buf*);
