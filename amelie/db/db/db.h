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

typedef struct Db Db;

struct Db
{
	Users  users;
	Wal    wal;
	Syncer syncer;
	// checkpoints
};

void db_init(Db*, UsersIf*, CatalogIf*, EvalIf*, void*);
void db_free(Db*);
void db_open(Db*, bool);
void db_close(Db*);
void db_state(Db*, Buf*);
