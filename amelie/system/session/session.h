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

typedef struct Session Session;

struct Session
{
	Vm           vm;
	Program*     program;
	Compiler     compiler;
	Dtr          dtr;
	Explain      explain;
	Content      content;
	Client*      client;
	int          lock_type;
	Lock*        lock;
	Lock*        lock_ref;
	Local        local;
	Frontend*    frontend;
	FrontendMgr* frontend_mgr;
};

Session*
session_create(Client*, FrontendMgr*, Frontend*);
void session_free(Session*);
void session_lock(Session*, int);
void session_unlock(Session*);
void session_main(Session*);
