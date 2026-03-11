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

bool table_storage_add(Table*, Tr*, Volume*, bool);
bool table_storage_drop(Table*, Tr*, Str*, bool);
bool table_storage_pause(Table*, Tr*, Str*, bool, bool);
