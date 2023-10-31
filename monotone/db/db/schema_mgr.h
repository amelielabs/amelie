#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SchemaMgr SchemaMgr;

struct SchemaMgr
{
	HandleMgr mgr;
};

void schema_mgr_init(SchemaMgr*);
void schema_mgr_free(SchemaMgr*);
void schema_mgr_create(SchemaMgr*, Transaction*, SchemaConfig*, bool);
void schema_mgr_drop(SchemaMgr*, Transaction*, Str*, bool);
void schema_mgr_rename(SchemaMgr*, Transaction*, Str*, Str*, bool);
void schema_mgr_dump(SchemaMgr*, Buf*);
Buf* schema_mgr_list(SchemaMgr*);
Schema*
schema_mgr_find(SchemaMgr*, Str*, bool);
