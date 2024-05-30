#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Backup Backup;

struct Backup
{
	int64_t    checkpoint_snapshot;
	WalSlot    wal_slot;
	Buf        state;
	Client*    client;
	Condition* on_complete;
	Task       task;
	Db*        db;
};

void backup_init(Backup*, Db*);
void backup_free(Backup*);
void backup_prepare(Backup*);
void backup_run(Backup*, Client*);
void backup(Db*, Client*);
