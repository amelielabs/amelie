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

typedef struct SchemaMgr SchemaMgr;

struct SchemaMgr
{
	RelationMgr mgr;
};

void schema_mgr_init(SchemaMgr*);
void schema_mgr_free(SchemaMgr*);
void schema_mgr_create(SchemaMgr*, Tr*, SchemaConfig*, bool);
void schema_mgr_drop(SchemaMgr*, Tr*, Str*, bool);
void schema_mgr_rename(SchemaMgr*, Tr*, Str*, Str*, bool);
void schema_mgr_dump(SchemaMgr*, Buf*);
Buf* schema_mgr_list(SchemaMgr*, Str*, bool);
Schema*
schema_mgr_find(SchemaMgr*, Str*, bool);
