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

typedef struct Storage Storage;

struct Storage
{
	Catalog users;
	Wal     wal;
	Syncer  syncer;
	// checkpoints
};

void storage_init(Storage*, CatalogIf*, EvalIf*, void*);
void storage_free(Storage*);
void storage_open(Storage*, bool);
void storage_close(Storage*);
void storage_state(Storage*, Buf*);
