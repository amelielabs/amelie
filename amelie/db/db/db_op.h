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

void db_gc(Db*);
void db_sync(Db*, uint64_t, bool);
void db_checkpoint(Db*);
void db_write(Db*, WriteList*);
