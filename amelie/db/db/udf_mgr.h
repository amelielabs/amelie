#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
Buf* udf_mgr_list(UdfMgr*);
Udf* udf_mgr_find(UdfMgr*, Str*, Str*, bool);
