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
	Tier* tiers;
	int   tiers_count;
	Uuid  id_table;
};

void tier_mgr_init(TierMgr*, Uuid*);
void tier_mgr_free(TierMgr*);
void tier_mgr_open(TierMgr*, StorageMgr*, List*);

static inline Tier*
tier_mgr_main(TierMgr* self)
{
	return self->tiers;
}
