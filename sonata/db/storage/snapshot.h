#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Snapshot Snapshot;

struct Snapshot
{
	Iov         iov;
	Buf         data;
	int         count;
	int         count_batch;
	SnapshotId* id;
	File        file;
};

void snapshot_init(Snapshot*);
void snapshot_free(Snapshot*);
void snapshot_reset(Snapshot*);
void snapshot_create(Snapshot*, SnapshotId*, Index*);
