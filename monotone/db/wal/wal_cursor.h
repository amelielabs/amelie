#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct WalCursor WalCursor;

struct WalCursor
{
	WalFile* file;
	uint64_t file_offset;
	Wal*     wal;
};

void wal_cursor_init(WalCursor*);
void wal_cursor_open(WalCursor*, Wal*, uint64_t);
void wal_cursor_close(WalCursor*);
Buf* wal_cursor_next(WalCursor*);

