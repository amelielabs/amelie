#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct HashStore HashStore;

#define HT_DELETED (void*)0xffffffff

struct HashStore
{
	Ref*     store;
	uint64_t count;
	uint64_t size;
	Keys*    keys;
};

always_inline hot static inline Ref*
hash_store_at(HashStore* self, int pos)
{
	auto ptr = (uint8_t*)self->store + pos * self->keys->key_size;
	return (Ref*)ptr;
}

always_inline hot static inline void
hash_store_copy(HashStore* self, Ref* dst, Ref* src)
{
	memcpy(dst, src, self->keys->key_size);
}

static inline void
hash_store_init(HashStore* self)
{
	self->store = NULL;
	self->count = 0;
	self->size  = 0;
	self->keys  = NULL;
}

static inline void
hash_store_free_rows(HashStore* self)
{
	if (self->count == 0)
		return;
	for (uint64_t pos = 0; pos < self->size; pos++)
	{
		auto key = hash_store_at(self, pos);
		if (!key->row || key->row == HT_DELETED)
			continue;
		row_free(key->row);
	}
}

static inline void
hash_store_free(HashStore* self)
{
	if (self->store)
	{
		if (self->keys->primary)
			hash_store_free_rows(self);
		am_free(self->store);
	}
	self->store = NULL;
	self->count = 0;
	self->size  = 0;
	self->keys  = NULL;
}

static inline bool
hash_store_is_full(HashStore* self)
{
	return self->count > self->size / 2;
}

static inline void
hash_store_create(HashStore* self, Keys* keys, size_t size)
{
	size_t allocated = keys->key_size * size;
	self->keys  = keys;
	self->size  = size;
	self->store = am_malloc(allocated);
	memset(self->store, 0, allocated);
}

hot static inline uint32_t
hash_store_hash(HashStore* self, Ref* row)
{
	uint32_t hash = 0;
	list_foreach(&self->keys->list)
	{
		auto key = list_at(Key, link);
		hash = key_hash(hash, ref_key(row, key->order));
	}
	return hash;
}

hot static inline bool
hash_store_set(HashStore* self, Ref* key, Ref* prev)
{
	uint64_t start = hash_store_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = hash_store_at(self, pos);
		if (!ref->row || ref->row == HT_DELETED)
		{
			hash_store_copy(self, ref, key);
			self->count++;
			return false;
		}
		if (! compare(self->keys, ref, key))
		{
			if (prev)
				hash_store_copy(self, prev, ref);
			hash_store_copy(self, ref, key);
			return true;
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return false;
}

hot static inline bool
hash_store_delete(HashStore* self, Ref* key, Ref* prev)
{
	uint64_t start = hash_store_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = hash_store_at(self, pos);
		if (! ref->row)
			break;
		if (ref->row != HT_DELETED)
		{
			if (! compare(self->keys, ref, key))
			{
				if (prev)
					hash_store_copy(self, prev, ref);
				ref->row = HT_DELETED;
				self->count--;
				return true;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	return false;
}

hot static inline Ref*
hash_store_get(HashStore* self, Ref* key, uint64_t* at)
{
	uint64_t start = hash_store_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = hash_store_at(self, pos);
		if (! ref->row)
		{
			*at = pos;
			return NULL;
		}
		if (ref->row == HT_DELETED)
		{
			*at = pos;
		} else
		{
			if (! compare(self->keys, ref, key))
			{
				*at = pos;
				return ref;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Ref*
hash_store_next(HashStore* self, uint64_t* at)
{
	uint64_t pos = *at;
	for (; pos < self->size; pos++)
	{
		auto ref = hash_store_at(self, pos);
		if (!ref->row || ref->row == HT_DELETED)
			continue;
		// set to next
		*at = pos;
		return ref;
	}
	*at = UINT64_MAX;
	return NULL;
}
