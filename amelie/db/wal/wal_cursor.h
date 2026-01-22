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
	Buf      buf;
	WalFile* file;
	uint64_t file_offset;
	bool     file_next;
	bool     crc;
	Wal*     wal;
};

void wal_cursor_init(WalCursor*);
void wal_cursor_open(WalCursor*, Wal*, uint64_t, bool, bool);
void wal_cursor_close(WalCursor*);
bool wal_cursor_active(WalCursor*);
bool wal_cursor_next(WalCursor*);
int  wal_cursor_readahead(WalCursor*, int, uint64_t*, uint64_t);
Record*
wal_cursor_at(WalCursor*);
