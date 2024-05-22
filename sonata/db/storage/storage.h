#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Storage Storage;

struct Storage
{
	List           indexes;
	int            indexes_count;
	SnapshotMgr    snapshot_mgr;
	StorageConfig* config;
	List           link_cp;
	List           link;
};

Storage*
storage_allocate(StorageConfig*);
void storage_free(Storage*);
void storage_open(Storage*, List*);
void storage_recover(Storage*);
void storage_gc(Storage*);
void storage_set(Storage*, Transaction*, bool, uint8_t**);
void storage_update(Storage*, Transaction*, Iterator*, uint8_t**);
void storage_delete(Storage*, Transaction*, Iterator*);
void storage_delete_by(Storage*, Transaction*, uint8_t**);
void storage_upsert(Storage*, Transaction*, Iterator**, uint8_t**);

Index*
storage_find(Storage*, Str*, bool);

static inline Index*
storage_primary(Storage* self)
{
	return container_of(self->indexes.next, Index, link);
}
