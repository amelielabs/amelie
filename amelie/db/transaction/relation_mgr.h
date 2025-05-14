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

typedef struct RelationMgr RelationMgr;

struct RelationMgr
{
	List list;
	int  list_count;
};

void      relation_mgr_init(RelationMgr*);
void      relation_mgr_free(RelationMgr*);
Relation* relation_mgr_get(RelationMgr*, Str*, Str*);
void      relation_mgr_replace(RelationMgr*, Relation*, Relation*);
void      relation_mgr_create(RelationMgr*, Tr*, Cmd, Relation*, Buf*);
void      relation_mgr_drop(RelationMgr*, Tr*, Cmd, Relation*, Buf*);
