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

void storage_checkpoint(Storage*);
void storage_checkpoint_db(Storage*, Db*);
void storage_checkpoint_catalog(Storage*);
void storage_sync(Storage*, uint64_t, bool);
void storage_write(Storage*, Writes*);
void storage_write_recover(Storage*, Buf*, uint64_t);
