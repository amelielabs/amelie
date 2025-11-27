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

typedef struct WalPeriodic WalPeriodic;

struct WalPeriodic
{
	int        interval_us;
	uint64_t   lsn;
	int64_t    coroutine_id;
	WalWorker* worker;
};

void wal_periodic_init(WalPeriodic*, WalWorker*);
void wal_periodic_start(WalPeriodic*);
void wal_periodic_stop(WalPeriodic*);
