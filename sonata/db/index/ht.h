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
	Row**    table;
	uint64_t count;
	uint64_t size;
	Keys*    keys;
};

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
	auto table = self->table;
	for (uint64_t pos = 0; pos < self->size; pos++)
	{
		auto row = table[pos];
		if (!row || row == HT_DELETED)
			continue;
		row_free(row);
	}
}

static inline void
ht_free(Ht* self)
{
	if (self->table)
	{
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

hot static inline uint32_t
ht_hash(Ht* self, Row* row)
{
	uint32_t hash = 0;
	list_foreach(&self->keys->list)
	{
		auto key = list_at(Key, link);
		hash = key_hash(hash, row_key(row, self->keys, key->order));
	}
	return hash;
}

static inline void
ht_create(Ht* self, Keys* keys, size_t size)
{
	size_t allocated = sizeof(Row*) * size;
	self->keys  = keys;
	self->size  = size;
	self->table = so_malloc(allocated);
	memset(self->table, 0, allocated);
}

hot static inline Row*
ht_set(Ht* self, Row* row)
{
	auto     table = self->table;
	uint64_t start = ht_hash(self, row) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (!ref || ref == HT_DELETED)
		{
			table[pos] = row;
			self->count++;
			return NULL;
		}
		if (! compare(self->keys, ref, row))
		{
			self->table[pos] = row;
			return ref;
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
ht_delete(Ht* self, Row* key)
{
	auto     table = self->table;
	uint64_t start = ht_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (! ref)
			break;
		if (ref != HT_DELETED)
		{
			if (! compare(self->keys, ref, key))
			{
				table[pos] = HT_DELETED;
				self->count--;
				return ref;
			}
		}
		pos = (pos + 1) % self->size;
	} while (start != pos);

	return NULL;
}

hot static inline Row*
ht_get(Ht* self, Row* key, uint64_t* at)
{
	auto     table = self->table;
	uint64_t start = ht_hash(self, key) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = table[pos];
		if (! ref)
		{
			*at = pos;
			return NULL;
		}

		if (ref == HT_DELETED)
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

hot static inline Row*
ht_next(Ht* self, uint64_t* at)
{
	uint64_t pos = *at;
	for (; pos < self->size; pos++)
	{
		auto ref = self->table[pos];
		if (!ref || ref == HT_DELETED)
			continue;
		// set to next
		*at = pos;
		return ref;
	}

	*at = UINT64_MAX;
	return NULL;
}
