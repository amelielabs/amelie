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

bool table_index_create(Table*, Tr*, IndexConfig*, bool);
void table_index_drop(Table*, Tr*, Str*, bool);
void table_index_rename(Table*, Tr*, Str*, Str*, bool);
