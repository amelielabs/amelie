#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct HashIterator HashIterator;

struct HashIterator
{
	Ref*       current;
	HashStore* current_store;
	uint64_t   pos;
	Hash*      hash;
};

static inline bool
hash_iterator_open(HashIterator* self, Hash* hash, Ref* key)
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
	self->current       = hash_store_at(hash->current, pos);
	self->current_store = hash->current;
	self->pos           = pos;
	self->hash          = hash;
}

static inline bool
hash_iterator_has(HashIterator* self)
{
	return self->current != NULL;
}

static inline Ref*
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

static inline void
hash_iterator_replace(HashIterator* self, Ref* key, Ref* prev)
{
	auto current_store = self->current_store;
	self->current = key;
	auto ref = hash_store_at(current_store, self->pos);
	hash_store_copy(current_store, prev, ref);
	hash_store_copy(current_store, ref, key);
}

static inline void
hash_iterator_delete(HashIterator* self, Ref* prev)
{
	auto current_store = self->current_store;
	auto ref = hash_store_at(current_store, self->pos);
	hash_store_copy(current_store, prev, ref);
	ref->row = HT_DELETED;
	current_store->count--;

	// keeping current as is
	self->current = prev;
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
