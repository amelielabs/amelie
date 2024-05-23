#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct SnapshotCursor SnapshotCursor;

struct SnapshotCursor
{
	uint64_t file_offset;
 	File     file;
};

void snapshot_cursor_init(SnapshotCursor*);
void snapshot_cursor_open(SnapshotCursor*, uint64_t, uint64_t);
void snapshot_cursor_close(SnapshotCursor*);
Buf* snapshot_cursor_next(SnapshotCursor*);
