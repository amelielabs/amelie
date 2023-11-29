#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Storage Storage;

struct Storage
{
	List           list;
	int            list_count;
	PartTree       tree;
	Uuid*          table;
	StorageMap*    map;
	StorageConfig* config;
	List           link;
};

Storage*
storage_allocate(StorageConfig*, StorageMap*, Uuid*);
void  storage_free(Storage*);
void  storage_open(Storage*, List*);
Part* storage_map(Storage*, uint8_t*, int, bool*);
