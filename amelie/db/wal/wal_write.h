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

typedef struct WalContext WalContext;

struct WalContext
{
	WriteList* list;
	uint64_t   lsn;
	uint64_t   sync_close;
	uint64_t   sync;
	bool       checkpoint;
};

void wal_write(Wal*, WalContext*);
