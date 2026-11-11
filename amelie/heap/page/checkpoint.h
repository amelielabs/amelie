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

typedef struct Checkpoint Checkpoint;

struct Checkpoint
{
	uint64_t lsn;
	char*    path;
	Uuid*    id;
	Buf*     schema;
	PageMgr* page_mgr;
};

void checkpoint_init(Checkpoint*);
void checkpoint_free(Checkpoint*);
void checkpoint_prepare(Checkpoint*, PageMgr*, Uuid*, Buf*);
bool checkpoint(Checkpoint*);
