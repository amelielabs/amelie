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

typedef struct RelMgr RelMgr;

struct RelMgr
{
	List list;
	int  list_count;
};

void rel_mgr_init(RelMgr*);
void rel_mgr_free(RelMgr*);
Rel* rel_mgr_get(RelMgr*, Str*, Str*);
void rel_mgr_replace(RelMgr*, Rel*, Rel*);
void rel_mgr_create(RelMgr*, Tr*, Rel*);
void rel_mgr_drop(RelMgr*, Tr*, Rel*);
