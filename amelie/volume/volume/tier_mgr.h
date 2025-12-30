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
	RelationMgr mgr;
};

void  tier_mgr_init(TierMgr*);
void  tier_mgr_free(TierMgr*);
bool  tier_mgr_create(TierMgr*, Tr*, Source*, bool);
bool  tier_mgr_drop(TierMgr*, Tr*, Str*, bool);
bool  tier_mgr_rename(TierMgr*, Tr*, Str*, Str*, bool);
void  tier_mgr_dump(TierMgr*, Buf*);
Buf*  tier_mgr_list(TierMgr*, Str*, bool);
Tier* tier_mgr_find(TierMgr*, Str*, bool);
