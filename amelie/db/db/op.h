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

void db_refresh(Db*, Uuid*, uint64_t, Str*);
void db_checkpoint(Db*);
void db_gc(Db*);
