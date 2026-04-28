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

bool table_mgr_create(Catalog*, Tr*, TableConfig*, bool);
bool table_mgr_drop(Catalog*, Tr*, Str*, Str*, bool);
void table_mgr_drop_of(Catalog*, Tr*, Table*);
bool table_mgr_truncate(Catalog*, Tr*, Str*, Str*, bool);
