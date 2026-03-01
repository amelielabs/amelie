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

typedef struct PartMgrIf PartMgrIf;
typedef struct PartMgr   PartMgr;

struct PartMgrIf
{
	void (*attach)(PartMgr*);
	void (*detach)(PartMgr*);
};

struct PartMgr
{
	PartMapping    mapping;
	List           list;
	int            list_count;
	PartMgrConfig* config;
	PartArg*       arg;
	PartMgrIf*     iface;
	void*          iface_arg;
	TierMgr*       tier_mgr;
};

void  part_mgr_init(PartMgr*, PartMgrIf*, void*, PartMgrConfig*,
                    PartArg*, TierMgr*, Keys*);
void  part_mgr_free(PartMgr*);
void  part_mgr_open(PartMgr*, List*);
void  part_mgr_close(PartMgr*);
void  part_mgr_drop(PartMgr*);
void  part_mgr_truncate(PartMgr*);
void  part_mgr_add(PartMgr*, Part*);
void  part_mgr_remove(PartMgr*, Part*);
Part* part_mgr_find(PartMgr*, uint64_t);
void  part_mgr_index_create(PartMgr*, IndexConfig*);
void  part_mgr_index_remove(PartMgr*, Str*);
Buf*  part_mgr_list(PartMgr*, Str*, int);
