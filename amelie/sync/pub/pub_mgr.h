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

typedef struct PubMgr PubMgr;

struct PubMgr
{
	int  list_count;
	List list;
};

void pub_mgr_init(PubMgr*);
void pub_mgr_free(PubMgr*);
void pub_mgr_add(PubMgr*, Pub*);
void pub_mgr_remove(PubMgr*, Pub*);
Pub* pub_mgr_find(PubMgr*, Uuid*, bool);
