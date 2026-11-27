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

typedef struct Apply   Apply;
typedef struct Compute Compute;

struct Apply
{
	Subscriber sub;
	Request    req;
	void*      session;
	Buf        buf;
	int64_t    worker_id;
	Compute*   compute;
};

void apply_init(Apply*, Compute*);
void apply_free(Apply*);
void apply_start(Apply*);
void apply_stop(Apply*);
