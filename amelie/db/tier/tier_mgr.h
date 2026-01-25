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
};

void tier_mgr_init(TierMgr*);
void tier_mgr_free(TierMgr*);
void tier_mgr_open(TierMgr*, StorageMgr*, List*);
