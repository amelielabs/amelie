#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PartList PartList;

struct PartList
{
	bool     reference;
	List     list;
	int      list_count;
	PartMap  map;
	PartMgr* mgr;
};

void  part_list_init(PartList*, PartMgr*);
void  part_list_free(PartList*);
void  part_list_create(PartList*, bool, List*, List*);
void  part_list_index_create(PartList*, IndexConfig*);
void  part_list_index_drop(PartList*, IndexConfig*);
Part* part_list_match(PartList*, Uuid*);
