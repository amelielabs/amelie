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
	Uuid      id;
	uint64_t  id_seq;
	Mapping   mapping;
	List      parts;
	int       parts_count;
	List      volumes;
	int       volumes_count;
	Sequence* seq;
	bool      unlogged;
	TierMgr*  tier_mgr;
};

void    volume_mgr_init(VolumeMgr*, TierMgr*, Sequence*, bool, Uuid*,
                        MappingConfig*, Keys*);
void    volume_mgr_free(VolumeMgr*);
void    volume_mgr_open(VolumeMgr*, List*, List*);
void    volume_mgr_truncate(VolumeMgr*);
void    volume_mgr_add(VolumeMgr*, Volume*, Part*);
void    volume_mgr_remove(VolumeMgr*, Part*);
void    volume_mgr_index_add(VolumeMgr*, IndexConfig*);
void    volume_mgr_index_remove(VolumeMgr*, Str*);
void    volume_mgr_set_unlogged(VolumeMgr*, bool);
Part*   volume_mgr_find(VolumeMgr*, uint64_t);
Volume* volume_mgr_find_volume(VolumeMgr*, Str*);

Iterator*
volume_mgr_iterator(VolumeMgr*, Part*, IndexConfig*, bool, Row*);
