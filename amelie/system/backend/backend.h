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

typedef struct Backend Backend;

struct Backend
{
	Vm      vm;
	TrList  prepared;
	TrCache cache;
	Route   route;
	Task    task;
	List    link;
};

Backend*
backend_allocate(Db*, FunctionMgr*, int);
void backend_free(Backend*);
void backend_start(Backend*);
void backend_stop(Backend*);
void backend_sync(Backend*);
