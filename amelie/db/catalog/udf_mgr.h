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

typedef struct UdfMgr UdfMgr;

struct UdfMgr
{
	RelationMgr mgr;
	UdfFree     free;
	void*       free_arg;
};

void udf_mgr_init(UdfMgr*, UdfFree, void*);
void udf_mgr_free(UdfMgr*);
bool udf_mgr_create(UdfMgr*, Tr*, UdfConfig*, bool);
bool udf_mgr_drop(UdfMgr*, Tr*, Str*, Str*, bool);
void udf_mgr_drop_of(UdfMgr*, Tr*, Udf*);
bool udf_mgr_rename(UdfMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void udf_mgr_dump(UdfMgr*, Buf*);
Buf* udf_mgr_list(UdfMgr*, Str*, Str*, bool);
Udf* udf_mgr_find(UdfMgr*, Str*, Str*, bool);
Udf* udf_mgr_find_depend(UdfMgr*, Str*, Str*);
