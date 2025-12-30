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

typedef struct VolumeMgr VolumeMgr;

struct VolumeMgr
{
	PartMap   map;
	List      list_parts;
	int       list_parts_count;
	List      list;
	int       list_count;
	Sequence* seq;
	bool      unlogged;
	TierMgr*  tier_mgr;
};

void    volume_mgr_init(VolumeMgr*, TierMgr*, Sequence*, bool);
void    volume_mgr_free(VolumeMgr*);
void    volume_mgr_create(VolumeMgr*, List*, List*, List*);
void    volume_mgr_map(VolumeMgr*);
void    volume_mgr_set_unlogged(VolumeMgr*, bool);
void    volume_mgr_truncate(VolumeMgr*);
void    volume_mgr_index_add(VolumeMgr*, IndexConfig*);
void    volume_mgr_index_drop(VolumeMgr*, Str*);
Volume* volume_mgr_find(VolumeMgr*, Str*);
Part*   volume_mgr_find_part(VolumeMgr*, uint64_t);

Iterator*
volume_mgr_iterator(VolumeMgr*, Part*, IndexConfig*, bool, Row*);
