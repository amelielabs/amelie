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
	void (*on_client)(Client*, void*);
	void (*on_native)(Native*, void*);
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
