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
bool   table_mgr_rename(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
bool   table_mgr_set_identity(TableMgr*, Tr*, Str*, Str*, int64_t, bool);
bool   table_mgr_set_unlogged(TableMgr*, Tr*, Str*, Str*, bool, bool);

// alter column
bool   table_mgr_column_rename(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool, bool);
Table* table_mgr_column_add(TableMgr*, Tr*, Str*, Str*, Column*, bool, bool);
Table* table_mgr_column_drop(TableMgr*, Tr*, Str*, Str*, Str*, bool, bool);
bool   table_mgr_column_set(TableMgr*, Tr*, Str*, Str*, Str*, Str*, int, bool, bool);
