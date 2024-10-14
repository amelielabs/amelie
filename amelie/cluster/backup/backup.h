#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Backup Backup;

struct Backup
{
	int64_t checkpoint_snapshot;
	WalSlot wal_slot;
	Buf     state;
	Client* client;
	Event   on_complete;
	Task    task;
	Db*     db;
};

void backup_init(Backup*, Db*);
void backup_free(Backup*);
void backup_run(Backup*, Client*);
void backup(Db*, Client*);
