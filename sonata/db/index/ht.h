#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Ht Ht;

#define HT_DELETED (void*)0xffffffff

struct Ht
{
	RowKey*  table;
	uint64_t count;
	uint64_t size;
	Keys*    keys;
};

always_inline hot static inline RowKey*
ht_at(Ht* self, int pos)
{
	uint8_t* ptr = (uint8_t*)self->table + pos * self->keys->key_size;
	return (RowKey*)ptr;
}

always_inline hot static inline void
ht_copy(Ht* self, RowKey* dst, RowKey* src)
{
	memcpy(dst, src, self->keys->key_size);
}

static inline void
ht_init(Ht* self)
{
	self->table = NULL;
	self->count = 0;
	self->size  = 0;
	self->keys  = NULL;
}

static inline void
ht_free_rows(Ht* self)
{
	if (self->count == 0)
		return;
	for (uint64_t pos = 0; pos < self->size; pos++)
	{
		auto key = ht_at(self, pos);
		if (!key->row || key->row == HT_DELETED)
			continue;
		row_free(key->row);
	}
}

static inline void
ht_free(Ht* self)
{
	if (self->table)
	{
		if (self->keys->primary)
			ht_free_rows(self);
		so_free(self->table);
	}
	self->table = NULL;
	self->count = 0;
	self->size  = 0;
	self->keys  = NULL;
}

static inline bool
ht_is_full(Ht* self)
{
	return self->count > self->size / 2;
}

static inline void
ht_create(Ht* self, Keys* keys, size_t size)
{
	size_t allocated = keys->key_size * size;
	self->keys  = keys;
	self->size  = size;
	self->table = so_malloc(allocated);
	memset(self->table, 0, allocated);
}

hot static inline uint32_t
ht_hash(Ht* self, RowKey* row)
{
	uint32_t hash = 0;
	list_foreach(&self->keys->list)
	{
		auto key = list_at(Key, link);
		hash = key_hash(hash, row_key(row, key->order));
	}
	return hash;
}

hot static inline Row*
ht_set(Ht* self, RowKey* key)
{
	uint64_t start = ht_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = ht_at(self, pos);
		if (!ref->row || ref->row == HT_DELETED)
		{
			ht_copy(self, ref, key);
			self->count++;
			return NULL;
		}
		if (! compare(self->keys, ref, key))
		{
			auto prev = ref->row;
			ht_copy(self, ref, key);
			return prev;
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
ht_delete(Ht* self, RowKey* key)
{
	uint64_t start = ht_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = ht_at(self, pos);
		if (! ref->row)
			break;
		if (ref->row != HT_DELETED)
		{
			if (! compare(self->keys, ref, key))
			{
				auto prev = ref->row;
				ref->row = HT_DELETED;
				self->count--;
				return prev;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	return NULL;
}

hot static inline Row*
ht_get(Ht* self, RowKey* key, uint64_t* at)
{
	uint64_t start = ht_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = ht_at(self, pos);
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
				return ref->row;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
ht_next(Ht* self, uint64_t* at)
{
	uint64_t pos = *at;
	for (; pos < self->size; pos++)
	{
		auto ref = ht_at(self, pos);
		if (!ref->row || ref->row == HT_DELETED)
			continue;
		// set to next
		*at = pos;
		return ref->row;
	}
	*at = UINT64_MAX;
	return NULL;
}
