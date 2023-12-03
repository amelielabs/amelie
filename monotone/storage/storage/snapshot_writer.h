#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SnapshotWriter SnapshotWriter;

struct SnapshotWriter
{
	Task task;
};

void snapshot_writer_init(SnapshotWriter*);
void snapshot_writer_start(SnapshotWriter*);
void snapshot_writer_stop(SnapshotWriter*);
void snapshot_write(SnapshotWriter*, Snapshot*);
