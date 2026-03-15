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

typedef struct SynonymMgr SynonymMgr;

struct SynonymMgr
{
	RelationMgr mgr;
};

void synonym_mgr_init(SynonymMgr*);
void synonym_mgr_free(SynonymMgr*);
bool synonym_mgr_create(SynonymMgr*, Tr*, SynonymConfig*, bool);
bool synonym_mgr_drop(SynonymMgr*, Tr*, Str*, Str*, bool);
void synonym_mgr_drop_of(SynonymMgr*, Tr*, Synonym*);
bool synonym_mgr_rename(SynonymMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void synonym_mgr_dump(SynonymMgr*, Buf*);
Buf* synonym_mgr_list(SynonymMgr*, Str*, Str*, int);
Synonym*
synonym_mgr_find(SynonymMgr*, Str*, Str*, bool);
