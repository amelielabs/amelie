#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Frontend Frontend;

typedef void (*FrontendEvent)(Frontend*, Client*);

struct Frontend
{
	LockMgr       lock_mgr;
	ClientMgr     client_mgr;
	Auth          auth;
	FrontendEvent on_connect;
	void*         on_connect_arg;
	Task          task;
};

void frontend_init(Frontend*, FrontendEvent, void*);
void frontend_free(Frontend*);
void frontend_start(Frontend*);
void frontend_stop(Frontend*);
void frontend_add(Frontend*, Buf*);
