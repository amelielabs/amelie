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

bool branch_mgr_create(Catalog*, Tr*, BranchConfig*, bool);
bool branch_mgr_drop(Catalog*, Tr*, Str*, Str*, bool);
void branch_mgr_drop_of(Catalog*, Tr*, Branch*);
bool branch_mgr_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool branch_mgr_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
