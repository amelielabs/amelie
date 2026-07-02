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

typedef struct Rels Rels;

struct Rels
{
	Hashtable ht;
	Hashtable htid;
	List      list;
	int       list_count;
};

void rels_init(Rels*);
void rels_free(Rels*);
void rels_replace(Rels*, Rel*, Rel*);
void rels_create(Rels*, Tr*, Rel*);
void rels_drop(Rels*, Tr*, Rel*);
void rels_rename(Rels*, Rel*, Str*, Str*);
void rels_dump(Rels*, RelType, Buf*, int);
void rels_list(Rels*, RelType, Buf*, Str*, Str*, bool, int);
void rels_list_rel(Rels*, Buf*, Str*, Str*, bool, int);
Rel* rels_find(Rels*, RelType, Str*, Str*, bool);
Rel* rels_find_by(Rels*, RelType, Uuid*, bool);
