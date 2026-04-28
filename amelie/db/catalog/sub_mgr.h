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

bool sub_mgr_create(Catalog*, Tr*, SubConfig*, bool);
bool sub_mgr_drop(Catalog*, Tr*, Str*, Str*, bool);
void sub_mgr_drop_of(Catalog*, Tr*, Sub*);
bool sub_mgr_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool sub_mgr_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
