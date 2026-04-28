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

typedef struct Catalog Catalog;

bool user_mgr_create(Catalog*, Tr*, UserConfig*, bool);
bool user_mgr_drop(Catalog*, Tr*, Str*, bool);
bool user_mgr_rename(Catalog*, Tr*, Str*, Str*, bool);
bool user_mgr_revoke(Catalog*, Tr*, Str*, Str*, bool);
bool user_mgr_grant(Catalog*, Tr*, Str*, bool, uint32_t, bool);
