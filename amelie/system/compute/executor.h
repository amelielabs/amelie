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

typedef struct Executor Executor;

struct Executor
{
	Subscriber sub;
	Portal     portal;
	Request    req;
	void*      session;
	Buf        buf;
	Compute*   compute;
};

void executor_init(Executor*, Compute*);
void executor_free(Executor*);
void executor_start(Executor*);
