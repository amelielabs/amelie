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

typedef struct PartMgr PartMgr;

struct PartMgr
{
	PartMapping mapping;
	List        parts;
	int         parts_count;
	Sequence*   seq;
	bool        unlogged;
	TierMgr*    tier_mgr;
};

void  part_mgr_init(PartMgr*, TierMgr*, Sequence*, bool, Keys*);
void  part_mgr_free(PartMgr*);
void  part_mgr_open(PartMgr*, List*);
void  part_mgr_add(PartMgr*, Part*);
void  part_mgr_remove(PartMgr*, Part*);
void  part_mgr_map(PartMgr*);
void  part_mgr_truncate(PartMgr*);
void  part_mgr_index_add(PartMgr*, IndexConfig*);
void  part_mgr_index_remove(PartMgr*, Str*);
void  part_mgr_set_unlogged(PartMgr*, bool);
Part* part_mgr_find(PartMgr*, uint64_t);
Buf*  part_mgr_status(PartMgr*, Str*, bool);
Iterator*
part_mgr_iterator(PartMgr*, Part*, IndexConfig*, bool, Row*);
