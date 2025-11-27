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
	Tr        tr;
	Write     write;
	WriteList write_list;
	bool      write_wal;
	uint64_t  ops;
	uint64_t  size;
	Storage*  storage;
};

void recover_init(Recover*, Storage*, bool);
void recover_free(Recover*);
void recover_next(Recover*, Record*);
void recover_wal(Recover*);
