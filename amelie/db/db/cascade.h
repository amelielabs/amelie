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

void cascade_schema_drop(Db*, Tr*, Str*, bool, bool);
void cascade_schema_rename(Db*, Tr*, Str*, Str*, bool);
