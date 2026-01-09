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
	Access   lock_catalog;
	Access   lock_catalog_exclusive;
	Lock     lock;
	Local    local;
};

Session*
session_create(void);
void session_free(Session*);
bool session_execute(Session*, Endpoint*, Str*, Output*);
