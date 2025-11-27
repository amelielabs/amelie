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

bool cascade_db_drop(Catalog*, Tr*, Str*, bool, bool);
bool cascade_db_rename(Catalog*, Tr*, Str*, Str*, bool);
