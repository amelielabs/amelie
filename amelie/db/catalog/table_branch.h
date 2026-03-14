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

bool table_branch_create(Table*, Tr*, Branch*, bool);
bool table_branch_drop(Table*, Tr*, Str*, bool);
