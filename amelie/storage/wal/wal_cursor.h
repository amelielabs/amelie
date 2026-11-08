#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct WalCursor WalCursor;

struct WalCursor
{
	WalFile* file;
	uint64_t file_offset;
	bool     file_next;
	bool     crc;
	bool     follow;
	Wal*     wal;
};

void wal_cursor_init(WalCursor*);
void wal_cursor_open(WalCursor*, Wal*, uint64_t, bool, bool, bool);
void wal_cursor_close(WalCursor*);
bool wal_cursor_active(WalCursor*);

RecordMsg*
wal_cursor_next_msg(WalCursor*);
Buf* wal_cursor_next(WalCursor*);
Buf* wal_cursor_readahead(WalCursor*, int, uint64_t*, uint64_t);
