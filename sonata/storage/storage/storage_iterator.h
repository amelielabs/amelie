#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct StorageIterator StorageIterator;

struct StorageIterator
{
	Row*      current;
	Iterator* part_iterator;
	Part*     part;
	Str*      index;
	Storage*  storage;
};

static inline void
storage_iterator_init(StorageIterator* self)
{
	self->current       = NULL;
	self->part_iterator = NULL;
	self->part          = NULL;
	self->index         = NULL;
	self->storage       = NULL;
}

static inline void
storage_iterator_next(StorageIterator* self);

static inline void
storage_iterator_open(StorageIterator* self,
                      Storage*         storage,
                      Str*             index_name,
                      Def*             def,
                      Row*             row)
{
	if (storage->list_count == 0)
		return;

	self->index   = index_name;
	self->storage = storage;

	// find partition
	if (storage->map->type == MAP_NONE ||
	    storage->map->type == MAP_REFERENCE)
	{
		self->part = storage_partition(storage);
	} else
	{
		// find partition by partition key
		uint8_t* pos = row_key(row, def, 0);
		int64_t key;
		data_read_integer(&pos, &key);
		self->part = part_tree_match(&storage->tree, key);
	}
	assert(self->part);

	// find partition index
	auto index = part_find(self->part, index_name, true);

	// create index iterator
	self->part_iterator = index_open(index, row, true);
	if (unlikely(! iterator_has(self->part_iterator)))
	{
		storage_iterator_next(self);
		return;
	}
	self->current = iterator_at(self->part_iterator);
}

static inline void
storage_iterator_open_as(StorageIterator* self,
                         Storage*         storage,
                         Str*             index_name,
                         Part*            part,
                         Iterator*        part_iterator)
{
	self->current       = iterator_at(part_iterator);
	self->part_iterator = part_iterator;
	self->part          = part;
	self->index         = index_name;
	self->storage       = storage;
}

static inline void
storage_iterator_close(StorageIterator* self)
{
	self->current = NULL;
	self->part    = NULL;
	self->index   = NULL;
	self->storage    = NULL;
	if (self->part_iterator)
	{
		iterator_close(self->part_iterator);
		self->part_iterator = NULL;
	}
}

static inline void
storage_iterator_reset(StorageIterator* self)
{
	storage_iterator_close(self);
}

hot static inline Row*
storage_iterator_at(StorageIterator* self)
{
	return self->current;
}

hot static inline bool
storage_iterator_has(StorageIterator* self)
{
	return self->current != NULL;
}

hot static inline void
storage_iterator_next(StorageIterator* self)
{
	if (self->part == NULL)
		return;

	// iterate current partition
	self->current = NULL;
	iterator_next(self->part_iterator);
	if (likely(iterator_has(self->part_iterator)))
	{
		self->current = iterator_at(self->part_iterator);
		return;
	}

	// close current iterator
	iterator_close(self->part_iterator);
	self->part_iterator = NULL;

	// match next non-empty partition 
	for (;;)
	{
		self->part = part_tree_next(&self->storage->tree, self->part);
		if (self->part == NULL)
			return;

		auto index = part_find(self->part, self->index, true);
		self->part_iterator = index_open(index, NULL, true);
		if (unlikely(! iterator_has(self->part_iterator)))
		{
			iterator_close(self->part_iterator);
			self->part_iterator = NULL;
			continue;
		}
		self->current = iterator_at(self->part_iterator);
		break;
	}
}
