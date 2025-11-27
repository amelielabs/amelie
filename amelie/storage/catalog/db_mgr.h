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

typedef struct DbMgr DbMgr;

struct DbMgr
{
	RelationMgr mgr;
};

void db_mgr_init(DbMgr*);
void db_mgr_free(DbMgr*);
bool db_mgr_create(DbMgr*, Tr*, DbConfig*, bool);
bool db_mgr_drop(DbMgr*, Tr*, Str*, bool);
bool db_mgr_rename(DbMgr*, Tr*, Str*, Str*, bool);
void db_mgr_dump(DbMgr*, Buf*);
Buf* db_mgr_list(DbMgr*, Str*, bool);
Db*  db_mgr_find(DbMgr*, Str*, bool);
