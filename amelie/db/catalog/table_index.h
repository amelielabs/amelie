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

void table_index_add(Table*, Tr*, IndexConfig*);
bool table_index_drop(Table*, Tr*, Str*, bool);
bool table_index_rename(Table*, Tr*, Str*, Str*, bool);
void table_index_list(Table*, Buf*, Str*, int);
IndexConfig*
table_index_find(Table*, Str*, bool);
