#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct StorageList StorageList;

struct StorageList
{
	List list;
	int  list_count;
};

static inline void
storage_list_init(StorageList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
storage_list_add(StorageList* self, Storage* storage)
{
	list_append(&self->list, &storage->link_list);
	self->list_count++;
}

static inline void
storage_list_remove(StorageList* self, Storage* storage)
{
	list_unlink(&storage->link);
	self->list_count--;
}

static inline Storage*
storage_list_find(StorageList* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto storage = list_at(Storage, link_list);
		if (uuid_compare(&storage->config->id, id))
			return storage;
	}
	return NULL;
}
