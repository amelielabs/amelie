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

typedef struct Cron Cron;

struct Cron
{
	int      interval_us;
	int64_t  coroutine_id;
	Service* service;
};

void cron_init(Cron*, Service*);
void cron_start(Cron*);
void cron_stop(Cron*);
