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
	PartMap   map;
	List      list;
	int       list_count;
	Sequence* seq;
	bool      unlogged;
};

void  part_mgr_init(PartMgr*);
void  part_mgr_free(PartMgr*);
void  part_mgr_create(PartMgr*, Source*, Sequence*, bool, List*, List*);
void  part_mgr_map(PartMgr*);
void  part_mgr_set_unlogged(PartMgr*, bool);
void  part_mgr_truncate(PartMgr*);
void  part_mgr_index_create(PartMgr*, IndexConfig*);
void  part_mgr_index_drop(PartMgr*, Str*);
Part* part_mgr_find(PartMgr*, uint64_t);
Iterator*
part_mgr_iterator(PartMgr*, Part*, IndexConfig*, bool, Row*);
