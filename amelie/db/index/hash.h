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

typedef struct Hash Hash;

struct Hash
{
	HashStore* current;
	HashStore* prev;
	HashStore  a, b;
	bool       rehashing;
	uint64_t   rehashing_pos;
	Keys*      keys;
};

static inline void
hash_init(Hash* self)
{
	self->current       = NULL;
	self->prev          = NULL;
	self->rehashing     = false;
	self->rehashing_pos = 0;
	self->keys          = NULL;
	hash_store_init(&self->a);
	hash_store_init(&self->b);
}

static inline void
hash_free(Hash* self)
{
	hash_store_free(&self->a);
	hash_store_free(&self->b);
	hash_init(self);
}

static inline void
hash_create(Hash* self, Keys* keys)
{
	self->keys = keys;
	self->current = &self->a;
	hash_store_create(self->current, keys, 256);
}

static inline void
hash_rehash_start(Hash* self)
{
	auto prev    = self->current;
	auto current = prev == &self->a ? &self->b : &self->a;

	// create new hash table and set as current
	hash_store_create(current, self->keys, prev->size * 2);
	self->current       = current;
	self->prev          = prev;
	self->rehashing     = true;
	self->rehashing_pos = 0;
}

hot static inline bool
hash_rehash(Hash* self)
{
	auto prev = self->prev;
	while (self->rehashing_pos < prev->size)
	{
		auto ref = prev->rows[self->rehashing_pos];
		if (!ref || ref == HT_DELETED)
		{
			self->rehashing_pos++;
			continue;
		}
		hash_store_set(self->current, ref);
		prev->rows[self->rehashing_pos] = NULL;
		prev->count--;

		self->rehashing_pos++;
		return true;
	}

	// complete
	hash_store_free(prev);
	self->prev = NULL;
	self->rehashing = false;
	return false;
}

hot static inline Row*
hash_set(Hash* self, Row* key)
{
	Row* prev = NULL;
	if (self->rehashing)
	{
		prev = hash_store_set(self->current, key);
		if (! prev)
			prev = hash_store_delete(self->prev, key);
		hash_rehash(self);
	} else
	{
		prev = hash_store_set(self->current, key);
		if (hash_store_is_full(self->current))
		{
			hash_rehash_start(self);
			hash_rehash(self);
		}
	}
	return prev;
}

hot static inline bool
hash_get_or_set(Hash* self, Row* key, uint64_t* pos)
{
	Row* prev = NULL;
	if (self->rehashing)
	{
		// get from the current and previous tables
		uint64_t current_pos = 0;
		prev = hash_store_get(self->current, key, &current_pos);
		if (! prev)
		{
			uint64_t prev_pos = 0;
			prev = hash_store_get(self->prev, key, &prev_pos);
			if (prev)
			{
				// move to the current
				self->current->rows[current_pos] = prev;
				self->prev->rows[prev_pos] = NULL;
				self->prev->count--;
				self->current->count++;
			}
		}

		// set to current, if not exists
		if (! prev)
		{
			self->current->rows[current_pos] = key;
			self->current->count++;
		}
		*pos = current_pos;

		hash_rehash(self);
	} else
	{
		uint64_t current_pos = 0;
		prev = hash_store_get(self->current, key, &current_pos);
		if (! prev)
		{
			self->current->rows[current_pos] = key;
			self->current->count++;
		}

		*pos = current_pos;
		if (hash_store_is_full(self->current))
		{
			hash_rehash_start(self);
			hash_rehash(self);
		}
	}

	return prev != NULL;
}

hot static inline Row*
hash_delete(Hash* self, Row* key)
{
	Row* prev = NULL;
	if (self->rehashing)
	{
		prev = hash_store_delete(self->current, key);
		if (! prev)
			prev = hash_store_delete(self->prev, key);
		hash_rehash(self);
	} else {
		prev = hash_store_delete(self->current, key);
	}
	return prev;
}
