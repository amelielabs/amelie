#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Snapshot Snapshot;

struct Snapshot
{
	uint64_t lsn;
	char     uuid[UUID_SZ];
	File     file;
};

void snapshot_init(Snapshot*);
void snapshot_create(Snapshot*, Uuid*, uint64_t);
void snapshot_complete(Snapshot*);
void snapshot_delete(Snapshot*);
void snapshot_write(Snapshot*, Iov*);
