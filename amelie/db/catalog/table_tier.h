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

bool table_tier_create(Table*, Tr*, TierConfig*, bool);
bool table_tier_drop(Table*, Tr*, Str*, bool);
bool table_tier_rename(Table*, Tr*, Str*, Str*, bool);
bool table_tier_storage_add(Table*, Tr*, Str*, Volume*, bool, bool);
bool table_tier_storage_drop(Table*, Tr*, Str*, Str*, bool, bool);
bool table_tier_storage_pause(Table*, Tr*, Str*, Str*, bool, bool, bool);
bool table_tier_set(Table*, Tr*, TierConfig*, int, bool);
Buf* table_tier_list(Table*, Str*, int);
