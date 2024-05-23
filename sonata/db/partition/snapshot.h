#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Snapshot Snapshot;

struct Snapshot
{
	Iov      iov;
	Buf      data;
	int      count;
	int      count_batch;
	uint64_t storage;
	uint64_t lsn;
	File     file;
};

void snapshot_init(Snapshot*);
void snapshot_free(Snapshot*);
void snapshot_reset(Snapshot*);
void snapshot_create(Snapshot*, Storage*, uint64_t);
