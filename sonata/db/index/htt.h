#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Htt Htt;

struct Htt
{
	Ht*      current;
	Ht*      prev;
	Ht       a, b;
	bool     rehashing;
	uint64_t rehashing_pos;
	Keys*    keys;
};

static inline void
htt_init(Htt* self)
{
	self->current       = NULL;
	self->prev          = NULL;
	self->rehashing     = false;
	self->rehashing_pos = 0;
	self->keys          = NULL;
	ht_init(&self->a);
	ht_init(&self->b);
}

static inline void
htt_free(Htt* self)
{
	ht_free(&self->a);
	ht_free(&self->b);
}

static inline void
htt_create(Htt* self, Keys* keys)
{
	self->keys = keys;
	self->current = &self->a;
	ht_create(self->current, keys, 256);
}

static inline void
htt_rehash_start(Htt* self)
{
	auto prev    = self->current;
	auto current = prev == &self->a ? &self->b : &self->a;

	// create new hash table and set as current
	ht_create(current, self->keys, prev->size * 2);
	self->current       = current;
	self->prev          = prev;
	self->rehashing     = true;
	self->rehashing_pos = 0;
}

hot static inline bool
htt_rehash(Htt* self)
{
	auto prev = self->prev;
	while (self->rehashing_pos < prev->size)
	{
		auto key = ht_at(prev, self->rehashing_pos);
		if (!key->row || key->row == HT_DELETED)
		{
			self->rehashing_pos++;
			continue;
		}
		ht_set(self->current, key);
		key->row = NULL;
		prev->count--;

		self->rehashing_pos++;
		return true;
	}

	// complete
	ht_free(prev);
	self->prev = NULL;
	self->rehashing = false;
	return false;
}

hot static inline Row*
htt_set(Htt* self, Row* row)
{
	uint8_t  key_data[self->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->keys, &hash);

	Row* result = NULL;
	if (self->rehashing)
	{
		// return previous row
		result = ht_set(self->current, key);
		if (! result)
			result = ht_delete(self->prev, key);

		htt_rehash(self);
	} else
	{
		result = ht_set(self->current, key);
		if (ht_is_full(self->current))
		{
			htt_rehash_start(self);
			htt_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
htt_get_or_set(Htt* self, Row* row, uint64_t* ht_pos)
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
		result = ht_get(self->current, key, &current_pos);
		if (! result)
		{
			uint64_t prev_pos = 0;
			result = ht_get(self->prev, key, &prev_pos);
			if (result)
			{
				// move to current
				auto prev = ht_at(self->prev, prev_pos);
				ht_copy(self->current, ht_at(self->current, current_pos), prev);
				prev->row = NULL;
				self->prev->count--;
				self->current->count++;
			}
		}

		// set to current, if not exists
		if (! result)
		{
			ht_copy(self->current, ht_at(self->current, current_pos), key);
			self->current->count++;
		}
		*ht_pos = current_pos;

		htt_rehash(self);
	} else
	{
		// get from current table
		uint64_t current_pos = 0;
		result = ht_get(self->current, key, &current_pos);

		// set to current, if not exists
		if (! result)
		{
			ht_copy(self->current, ht_at(self->current, current_pos), key);
			self->current->count++;
		}
		*ht_pos = current_pos;

		if (ht_is_full(self->current))
		{
			htt_rehash_start(self);
			htt_rehash(self);
		}
	}
	return result;
}

hot static inline Row*
htt_delete(Htt* self, Row* row)
{
	uint8_t  key_data[self->keys->key_size];
	auto     key  = (RowKey*)key_data;
	uint32_t hash = 0;
	row_key_create_and_hash(key, row, self->keys, &hash);

	Row* result = NULL;
	if (self->rehashing)
	{
		result = ht_delete(self->current, key);
		if (! result)
			result = ht_delete(self->prev, key);

		htt_rehash(self);
	} else
	{
		result = ht_delete(self->current, key);
	}
	return result;
}
