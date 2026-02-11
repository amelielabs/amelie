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

typedef struct Backup Backup;

struct Backup
{
	int      state_step;
	int      state_step_total;
	uint8_t* state_pos;
	Buf      state;
	Buf*     state_file;
	bool     snapshot;
	WalSlot  wal_slot;
	Client*  client;
	Event    on_complete;
	Task     task;
	Db*      db;
};

void backup_init(Backup*, Db*);
void backup_free(Backup*);
void backup_run(Backup*, Client*);
void backup(Db*, Client*);
