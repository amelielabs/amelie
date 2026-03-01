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

typedef struct Tier Tier;

struct Tier
{
	Mapping     mapping;
	List        list;
	int         list_count;
	TierConfig* config;
	List        link;
};

Tier*   tier_allocate(TierConfig*, Keys*);
void    tier_free(Tier*);
void    tier_open(Tier*, StorageMgr*);
void    tier_add(Tier*, Object*);
void    tier_remove(Tier*, Object*);
void    tier_truncate(Tier*);
void    tier_drop(Tier*);
Object* tier_find(Tier*, uint64_t);

static inline bool
tier_empty(Tier* self)
{
	return !self->list_count;
}
