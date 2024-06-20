#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
		auto key = hash_store_at(prev, self->rehashing_pos);
		if (!key->row || key->row == HT_DELETED)
		{
			self->rehashing_pos++;
			continue;
		}
		hash_store_set(self->current, key);
		key->row = NULL;
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
hash_set(Hash* self, Row* row)
{
	uint8_t  key_data[self->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->keys, &hash);

	Row* result = NULL;
	if (self->rehashing)
	{
		// return previous row
		result = hash_store_set(self->current, key);
		if (! result)
			result = hash_store_delete(self->prev, key);

		hash_rehash(self);
	} else
	{
		result = hash_store_set(self->current, key);
		if (hash_store_is_full(self->current))
		{
			hash_rehash_start(self);
			hash_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
hash_get_or_set(Hash* self, Row* row, uint64_t* hash_store_pos)
{
	uint8_t  key_data[self->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->keys, &hash);

	Row* result = NULL;
	if (self->rehashing)
	{
		// get from current and previous tables
		uint64_t current_pos = 0;
		result = hash_store_get(self->current, key, &current_pos);
		if (! result)
		{
			uint64_t prev_pos = 0;
			result = hash_store_get(self->prev, key, &prev_pos);
			if (result)
			{
				// move to current
				auto prev = hash_store_at(self->prev, prev_pos);
				hash_store_copy(self->current,
				                hash_store_at(self->current, current_pos),
				                prev);
				prev->row = NULL;
				self->prev->count--;
				self->current->count++;
			}
		}

		// set to current, if not exists
		if (! result)
		{
			hash_store_copy(self->current,
			                hash_store_at(self->current, current_pos),
			                key);
			self->current->count++;
		}
		*hash_store_pos = current_pos;

		hash_rehash(self);
	} else
	{
		// get from current table
		uint64_t current_pos = 0;
		result = hash_store_get(self->current, key, &current_pos);

		// set to current, if not exists
		if (! result)
		{
			hash_store_copy(self->current,
			                hash_store_at(self->current, current_pos),
			                key);
			self->current->count++;
		}
		*hash_store_pos = current_pos;

		if (hash_store_is_full(self->current))
		{
			hash_rehash_start(self);
			hash_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
hash_delete(Hash* self, Row* row)
{
	uint8_t  key_data[self->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->keys, &hash);

	Row* result = NULL;
	if (self->rehashing)
	{
		result = hash_store_delete(self->current, key);
		if (! result)
			result = hash_store_delete(self->prev, key);

		hash_rehash(self);
	} else
	{
		result = hash_store_delete(self->current, key);
	}
	return result;
}
