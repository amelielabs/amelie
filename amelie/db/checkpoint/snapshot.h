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

typedef struct Snapshot Snapshot;

struct Snapshot
{
	Iov      iov;
	Buf      data;
	int      count;
	int      count_batch;
	uint64_t partition;
	uint64_t lsn;
	File     file;
};

void snapshot_init(Snapshot*);
void snapshot_free(Snapshot*);
void snapshot_create(Snapshot*, Part*, uint64_t);
