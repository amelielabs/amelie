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

typedef struct TierMgr TierMgr;

struct TierMgr
{
	List        list;
	int         list_count;
	Keys*       keys;
	StorageMgr* storage_mgr;
};

void  tier_mgr_init(TierMgr*, StorageMgr*, Keys*);
void  tier_mgr_free(TierMgr*);
void  tier_mgr_open(TierMgr*, List*);
void  tier_mgr_load(TierMgr*);
void  tier_mgr_drop(TierMgr*);
void  tier_mgr_truncate(TierMgr*);
Tier* tier_mgr_create(TierMgr*, TierConfig*);
void  tier_mgr_remove(TierMgr*, Tier*);
void  tier_mgr_remove_by(TierMgr*, Str*);
Tier* tier_mgr_find(TierMgr*, Str*);
Tier* tier_mgr_find_by(TierMgr*, Volume*);
Id*   tier_mgr_find_object(TierMgr*, Tier**, uint64_t);
Buf*  tier_mgr_list(TierMgr*, Str*, int);

static inline Tier*
tier_mgr_first(TierMgr* self)
{
	return container_of(list_first(&self->list), Tier, link);
}

static inline bool
tier_mgr_empty(TierMgr* self)
{
	return tier_mgr_first(self)->list_pending_count > 0;
}
