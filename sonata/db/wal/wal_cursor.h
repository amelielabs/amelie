#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct WalCursor WalCursor;

struct WalCursor
{
	Buf      buf;
	WalFile* file;
	uint64_t file_offset;
	Wal*     wal;
};

void wal_cursor_init(WalCursor*);
void wal_cursor_open(WalCursor*, Wal*, uint64_t);
void wal_cursor_close(WalCursor*);
bool wal_cursor_next(WalCursor*);
WalWrite*
wal_cursor_at(WalCursor*);
