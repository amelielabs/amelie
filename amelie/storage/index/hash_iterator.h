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

typedef struct HashIterator HashIterator;

struct HashIterator
{
	Row*       current;
	HashStore* current_store;
	uint64_t   pos;
	Hash*      hash;
};

static inline bool
hash_iterator_open(HashIterator* self, Row* key)
{
	self->current       = NULL;
	self->current_store = NULL;
	self->pos           = 0;

	// point-lookup
	auto hash = self->hash;
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
			self->pos           = 0;
			self->current_store = self->hash->prev;
			self->current       = hash_store_next(self->current_store, &self->pos);
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
hash_iterator_open_at(HashIterator* self, uint64_t pos)
{
	auto hash = self->hash;
	self->current       = hash->current->rows[pos];
	self->current_store = hash->current;
	self->pos           = pos;
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
hash_iterator_replace(HashIterator* self, Row* key)
{
	auto current_store = self->current_store;
	auto prev = current_store->rows[self->pos];
	current_store->rows[self->pos] = key;
	self->current = key;
	return prev;
}

static inline Row*
hash_iterator_delete(HashIterator* self)
{
	auto current_store = self->current_store;
	auto prev = current_store->rows[self->pos];
	current_store->rows[self->pos] = HT_DELETED;
	current_store->count--;
	// keeping current as is
	self->current = prev;
	return prev;
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
hash_iterator_init(HashIterator* self, Hash* hash)
{
	self->current       = NULL;
	self->current_store = NULL;
	self->pos           = 0;
	self->hash          = hash;
}
