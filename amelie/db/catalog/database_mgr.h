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

typedef struct DatabaseMgr DatabaseMgr;

struct DatabaseMgr
{
	RelationMgr mgr;
};

void database_mgr_init(DatabaseMgr*);
void database_mgr_free(DatabaseMgr*);
bool database_mgr_create(DatabaseMgr*, Tr*, DatabaseConfig*, bool);
bool database_mgr_drop(DatabaseMgr*, Tr*, Str*, bool);
bool database_mgr_rename(DatabaseMgr*, Tr*, Str*, Str*, bool);
void database_mgr_dump(DatabaseMgr*, Buf*);
Buf* database_mgr_list(DatabaseMgr*, Str*, bool);
Database*
database_mgr_find(DatabaseMgr*, Str*, bool);
