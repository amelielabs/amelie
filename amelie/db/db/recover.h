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

typedef struct Recover Recover;

struct Recover
{
	RecoverSync sync;
	Db*         db;
};

void recover_init(Recover*, Db*);
void recover_free(Recover*);
void recover_wait(Recover*);
void recover_next(Recover*, RecordMsg*);
void recover_wal(Recover*);
