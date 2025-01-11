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

void   table_mgr_rename(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_set_unlogged(TableMgr*, Tr*, Str*, Str*, bool, bool);
void   table_mgr_column_rename(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
Table* table_mgr_column_add(TableMgr*, Tr*, Str*, Str*, Column*, bool);
Table* table_mgr_column_drop(TableMgr*, Tr*, Str*, Str*, Str*, bool);
void   table_mgr_column_set_default(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_column_set_identity(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_column_set_stored(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void   table_mgr_column_set_resolved(TableMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
