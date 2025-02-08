#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
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
