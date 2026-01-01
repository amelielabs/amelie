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

typedef struct Engine Engine;

struct Engine
{
	Uuid      id;
	PartMap   map;
	List      parts;
	int       parts_count;
	List      volumes;
	int       volumes_count;
	Sequence* seq;
	bool      unlogged;
	TierMgr*  tier_mgr;
};

void    engine_init(Engine*, Uuid*, TierMgr*, Sequence*, bool);
void    engine_free(Engine*);
void    engine_open(Engine*, List*, List*);
void    engine_prepare(Engine*, List*);
void    engine_truncate(Engine*);
void    engine_index_add(Engine*, IndexConfig*);
void    engine_index_drop(Engine*, Str*);
void    engine_set_unlogged(Engine*, bool);
Part*   engine_find(Engine*, uint64_t);
Volume* engine_find_volume(Engine*, Str*);

Iterator*
engine_iterator(Engine*, Part*, IndexConfig*, bool, Row*);
