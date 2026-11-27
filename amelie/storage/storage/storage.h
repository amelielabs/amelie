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
	Catalog  catalog;
	uint64_t catalog_lsn;
	Service  service;
	Wal      wal;
	Syncer   syncer;
	EvalIf*  iface;
	void*    iface_arg;
};

void storage_init(Storage*, CatalogIf*, EvalIf*, void*);
void storage_free(Storage*);
void storage_start(Storage*, bool);
void storage_stop(Storage*);
void storage_state(Storage*, Buf*);
