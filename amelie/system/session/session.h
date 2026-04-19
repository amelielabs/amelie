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
	Dtr      dtr;
	Request* req;
	SetCache set_cache;
	Profile  profile;
	Local    local;
};

Session*
session_create(void);
void session_free(Session*);
bool session_execute(Session*, Request*);
