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

typedef struct PartList PartList;

struct PartList
{
	bool    unlogged;
	List    list;
	int     list_count;
	PartMap map;
};

void  part_list_init(PartList*);
void  part_list_free(PartList*);
void  part_list_create(PartList*, bool, Sequence*, List*, List*);
void  part_list_map(PartList*);
void  part_list_set_unlogged(PartList*, bool);
void  part_list_truncate(PartList*);
void  part_list_index_create(PartList*, IndexConfig*);
void  part_list_index_drop(PartList*, IndexConfig*);
Part* part_list_match(PartList*, Core*);
Iterator*
part_list_iterator(PartList*, Part*, IndexConfig*, bool, Row*);
