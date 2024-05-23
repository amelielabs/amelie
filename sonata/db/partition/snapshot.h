#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Snapshot Snapshot;

struct Snapshot
{
	Iov      iov;
	Buf      data;
	int      count;
	int      count_batch;
	uint64_t partition;
	uint64_t lsn;
	File     file;
};

void snapshot_init(Snapshot*);
void snapshot_free(Snapshot*);
void snapshot_reset(Snapshot*);
void snapshot_create(Snapshot*, Part*, uint64_t);
