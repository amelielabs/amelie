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
	void (*create)(Recover*);
	void (*free)(Recover*);
	void (*execute)(Recover*, RecordMsg*);
	void (*sync)(Recover*);
};

struct Recover
{
	RecoverIf* iface;
	void*      iface_arg;
	void*      state;
	Db*        db;
};

void recover_init(Recover*, Db*, RecoverIf*, void*);
void recover_free(Recover*);
void recover_next(Recover*, Record*);
void recover_wal(Recover*);
