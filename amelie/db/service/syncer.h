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

typedef struct Syncer Syncer;

struct Syncer
{
	int      interval_us;
	int64_t  coroutine_id;
	Service* service;
};

void syncer_init(Syncer*, Service*);
void syncer_start(Syncer*);
void syncer_stop(Syncer*);
