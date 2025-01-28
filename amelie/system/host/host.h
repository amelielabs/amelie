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

typedef struct Host Host;

typedef void (*HostEvent)(Host*, Client*);

struct Host
{
	LockMgr   lock_mgr;
	ClientMgr client_mgr;
	Auth      auth;
	HostEvent on_connect;
	void*     on_connect_arg;
	Task      task;
};

void host_init(Host*, HostEvent, void*);
void host_free(Host*);
void host_start(Host*);
void host_stop(Host*);
void host_add(Host*, Buf*);
