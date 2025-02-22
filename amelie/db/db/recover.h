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

typedef struct RecoverIf RecoverIf;
typedef struct Recover   Recover;

struct RecoverIf
{
	void (*build_index)(Recover*, Table*, IndexConfig*);
	void (*build_column_add)(Recover*, Table*, Table*, Column*);
	void (*build_column_drop)(Recover*, Table*, Table*, Column*);
};

struct Recover
{
	Tr         tr;
	Write      write;
	WriteList  write_list;
	bool       write_wal;
	uint64_t   ops;
	uint64_t   size;
	RecoverIf* iface;
	void*      iface_arg;
	Db*        db;
};

void recover_init(Recover*, Db*, bool, RecoverIf*, void*);
void recover_free(Recover*);
void recover_next(Recover*, Record*);
void recover_wal(Recover*);
