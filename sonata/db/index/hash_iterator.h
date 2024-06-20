#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct HashIterator HashIterator;

struct HashIterator
{
	Row*       current;
	HashStore* current_store;
	uint64_t   pos;
	Hash*      hash;
};

static inline bool
hash_iterator_open(HashIterator* self, Hash* hash, RowKey* key)
{
	self->current       = NULL;
	self->current_store = NULL;
	self->pos           = 0;
	self->hash          = hash;

	// point-lookup
	if (key)
	{
		self->current_store = hash->current;
		self->current = hash_store_get(hash->current, key, &self->pos);
		if (self->current)
			return true;

		if (hash->rehashing)
		{
			self->current_store = hash->prev;
			self->current = hash_store_get(hash->prev, key, &self->pos);
			if (self->current)
				return true;
		}

		return false;
	}

	// full scan
	if (hash->rehashing)
	{
		self->current_store = self->hash->current;
		self->current = hash_store_next(self->current_store, &self->pos);
		if (! self->current)
		{
			self->pos        = 0;
			self->current_store = self->hash->prev;
			self->current    = hash_store_next(self->current_store, &self->pos);
		}
	} else
	{
		if (hash->current->count == 0)
			return false;
		self->current_store = self->hash->current;
		self->current = hash_store_next(self->current_store, &self->pos);
	}
	return false;
}

static inline void
hash_iterator_open_at(HashIterator* self, Hash* hash, uint64_t pos)
{
	self->current       = hash_store_at(hash->current, pos)->row;
	self->current_store = hash->current;
	self->pos           = pos;
	self->hash          = hash;
}

static inline bool
hash_iterator_has(HashIterator* self)
{
	return self->current != NULL;
}

static inline Row*
hash_iterator_at(HashIterator* self)
{
	return self->current;
}

static inline void
hash_iterator_next(HashIterator* self)
{
	if (self->current == NULL)
		return;
	auto hash = self->hash;

	self->pos++;
	self->current = hash_store_next(self->current_store, &self->pos);
	if (self->current)
		return;

	if (! hash->rehashing)
		return;

	if (self->current_store == hash->prev)
		return;
	self->pos           = 0;
	self->current_store = hash->prev;
	self->current       = hash_store_next(self->current_store, &self->pos);
}

static inline Row*
hash_iterator_replace(HashIterator* self, Row* row)
{
	uint8_t  key_data[self->hash->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->hash->keys, &hash);

	auto current = self->current;
	assert(current);
	self->current = row;
	hash_store_copy(self->current_store, hash_store_at(self->current_store, self->pos), key);
	return current;
}

static inline Row*
hash_iterator_delete(HashIterator* self)
{
	// keeping current as is
	auto current = self->current;
	auto current_store = self->current_store;
	hash_store_at(self->current_store, self->pos)->row = HT_DELETED;
	current_store->count--;
	return current;
}

static inline void
hash_iterator_reset(HashIterator* self)
{
	self->current       = NULL;
	self->current_store = NULL;
	self->pos           = 0;
	self->hash          = NULL;
}

static inline void
hash_iterator_close(HashIterator* self)
{
	hash_iterator_reset(self);
}

static inline void
hash_iterator_init(HashIterator* self)
{
	self->current       = NULL;
	self->current_store = NULL;
	self->pos           = 0;
	self->hash          = NULL;
}
