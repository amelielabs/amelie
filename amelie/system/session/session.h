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
	Compiler compiler;
	Vm       vm;
	Program* program;
	SetCache set_cache;
	Dtr      dtr;
	Profile  profile;
	LockId   lock;
	Local    local;
};

Session*
session_create(void);
void session_free(Session*);
void session_lock(Session*, LockId);
void session_unlock(Session*);
bool session_execute(Session*, Endpoint*, Str*, Output*);
