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
	UdfIf*    iface;
	void*     iface_arg;
	HandleMgr mgr;
};

void udf_mgr_init(UdfMgr*, UdfIf*, void*);
void udf_mgr_free(UdfMgr*);
void udf_mgr_create(UdfMgr*, Tr*, UdfConfig*, bool);
void udf_mgr_drop(UdfMgr*, Tr*, Str*, Str*, bool);
void udf_mgr_rename(UdfMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void udf_mgr_dump(UdfMgr*, Buf*);
Buf* udf_mgr_list(UdfMgr*, Str*, Str*, bool);
Udf* udf_mgr_find(UdfMgr*, Str*, Str*, bool);
