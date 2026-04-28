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

// alter table
bool table_mgr_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool table_mgr_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);
bool table_mgr_set_identity(Catalog*, Tr*, Str*, Str*, int64_t, bool);

// alter column
bool table_mgr_column_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool, bool);
bool table_mgr_column_add(Catalog*, Tr*, Str*, Str*, Column*, bool, bool);
bool table_mgr_column_drop(Catalog*, Tr*, Str*, Str*, Str*, bool, bool);
bool table_mgr_column_set(Catalog*, Tr*, Str*, Str*, Str*, Str*, int, bool, bool);
