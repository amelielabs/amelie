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

typedef struct Io Io;

typedef void (*IoEvent)(Io*, Client*);

struct Io
{
	LockMgr   lock_mgr;
	ClientMgr client_mgr;
	Auth      auth;
	IoEvent   on_connect;
	void*     on_connect_arg;
	Task      task;
};

void io_init(Io*, IoEvent, void*);
void io_free(Io*);
void io_start(Io*);
void io_stop(Io*);
void io_add(Io*, Buf*);
