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
	Query*       query;
	QueryContext query_context;
	QueryMgr     query_mgr;
	SetCache     set_cache;
	Dtr          dtr;
	Profile      profile;
	int          lock_type;
	Lock*        lock;
	Lock*        lock_ref;
	Local        local;
	Frontend*    frontend;
};

Session*
session_create(Frontend*);
void session_free(Session*);
void session_lock(Session*, int);
void session_unlock(Session*);
bool session_execute(Session*, Str*, Str*, Str*, Content*);
void session_execute_replay(Session*, Primary*, Buf*);
