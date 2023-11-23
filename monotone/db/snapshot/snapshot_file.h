#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SnapshotFile SnapshotFile;

struct SnapshotFile
{
	uint64_t id;
	char     uuid[UUID_SZ];
 	File     file;
};

void snapshot_file_init(SnapshotFile*);
void snapshot_file_create(SnapshotFile*, Uuid*, uint64_t);
void snapshot_file_complete(SnapshotFile*);
void snapshot_file_abort(SnapshotFile*);
void snapshot_file_write(SnapshotFile*, Iov*);
