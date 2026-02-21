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

void   engine_add(Engine*, Id*);
void   engine_remove(Engine*, Id*);
void   engine_set_unlogged(Engine*, bool);
void   engine_truncate(Engine*);
void   engine_index_create(Engine*, IndexConfig*);
void   engine_index_remove(Engine*, Str*);
void   engine_tier_add(Engine*, Tier*);
void   engine_tier_remove(Engine*, Str*);
Level* engine_tier_find(Engine*, Str*);
Level* engine_tier_find_by(Engine*, Tier*);
Id*    engine_find(Engine*, uint64_t);
