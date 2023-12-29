#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct SnapshotCursor SnapshotCursor;

struct SnapshotCursor
{
	uint64_t file_offset;
 	File     file;
};

void snapshot_cursor_init(SnapshotCursor*);
void snapshot_cursor_open(SnapshotCursor*, SnapshotId*);
void snapshot_cursor_close(SnapshotCursor*);
Buf* snapshot_cursor_next(SnapshotCursor*);
