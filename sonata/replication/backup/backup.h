#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Backup Backup;

struct Backup
{
	int64_t    wal_snapshot;
	int64_t    checkpoint_snapshot;
	Buf        buf_state;
	Buf        buf;
	Tcp*       tcp;
	Condition* on_complete;
	Task       task;
	Db*        db;
};

void backup_init(Backup*, Db*);
void backup_free(Backup*);
void backup_prepare(Backup*);
void backup_run(Backup*, Tcp*);
void backup(Db*, Tcp*);
