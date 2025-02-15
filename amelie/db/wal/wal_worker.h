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

typedef struct WalWorker WalWorker;

enum
{
	WAL_SHUTDOWN = 1 << 0,
	WAL_SYNC     = 1 << 1,
	WAL_ROTATE   = 1 << 2
};

struct WalWorker
{
	Mutex   lock;
	CondVar cond_var;
	int     pending;
	Wal*    wal;
	Task    task;
};

void wal_worker_init(WalWorker*, Wal*);
void wal_worker_free(WalWorker*);
void wal_worker_start(WalWorker*);
void wal_worker_stop(WalWorker*);
void wal_worker_schedule(WalWorker*, int);
