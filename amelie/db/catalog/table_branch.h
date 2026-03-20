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

bool    table_branch_create(Table*, Tr*, Branch*, bool);
bool    table_branch_drop(Table*, Tr*, Str*, bool);
bool    table_branch_rename(Table*, Tr*, Str*, Str*, bool);
Buf*    table_branch_list(Table*, Str*, int);
Branch* table_branch_find(Table*, Str*, bool);
Branch* table_branch_find_by(Table*, uint32_t, bool);
