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
	Vm        vm;
	Executor* executor;
	Task      task;
	List      link;
};

void backend_init(Backend*, Db*, Executor*, FunctionMgr*);
void backend_free(Backend*);
void backend_start(Backend*);
void backend_stop(Backend*);
