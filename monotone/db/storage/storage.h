#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Storage Storage;

struct Storage
{
	int            refs;
	List           indexes;
	int            indexes_count;
	StorageConfig* config;
	List           link_list;
	List           link;
};

Storage*
storage_allocate(StorageConfig*);
void storage_free(Storage*);
void storage_ref(Storage*);
void storage_unref(Storage*);
void storage_attach(Storage*, Index*);
void storage_detach(Storage*, Index*);
void storage_write(Storage*, Transaction*, LogCmd, bool, uint8_t*, int);
Index*
storage_find(Storage*, Str*, bool);

static inline Index*
storage_primary(Storage* self)
{
	return container_of(self->indexes.next, Index, link);
}
