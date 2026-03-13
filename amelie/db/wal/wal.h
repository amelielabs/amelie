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

typedef struct Wal Wal;

struct Wal
{
	Spinlock lock;
	WalFile* current;
	WalFile* files;
	int      files_count;
	List     slots;
	int      slots_count;
	List     subscribes;
	int      subscribes_count;
	int      dirfd;
};

void     wal_init(Wal*);
void     wal_free(Wal*);
void     wal_open(Wal*);
void     wal_close(Wal*);
void     wal_gc(Wal*, uint64_t);
WalFile* wal_find(Wal*, uint64_t, bool);
Buf*     wal_status(Wal*);
