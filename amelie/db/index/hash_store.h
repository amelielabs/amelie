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

typedef struct HashStore HashStore;

#define HT_DELETED (void*)0xffffffff

struct HashStore
{
	Row**    rows;
	uint64_t count;
	uint64_t size;
	Keys*    keys;
};

static inline void
hash_store_init(HashStore* self)
{
	self->rows  = NULL;
	self->count = 0;
	self->size  = 0;
	self->keys  = NULL;
}

static inline void
hash_store_free(HashStore* self)
{
	if (self->rows)
		am_free(self->rows);
	self->rows  = NULL;
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
	size_t allocated = sizeof(Row*) * size;
	self->keys = keys;
	self->size = size;
	self->rows = am_malloc(allocated);
	memset(self->rows, 0, allocated);
}

hot static inline Row*
hash_store_set(HashStore* self, Row* key)
{
	uint64_t start = row_hash(key, self->keys) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = self->rows[pos];
		if (!ref || ref == HT_DELETED)
		{
			self->rows[pos] = key;
			self->count++;
			return NULL;
		}

		if (! compare(self->keys, ref, key))
		{
			self->rows[pos] = key;
			return ref;
		}

		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return false;
}

hot static inline Row*
hash_store_delete(HashStore* self, Row* key)
{
	uint64_t start = row_hash(key, self->keys) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = self->rows[pos];
		if (! ref)
			break;

		if (ref != HT_DELETED)
		{
			if (! compare(self->keys, ref, key))
			{
				self->rows[pos] = HT_DELETED;
				self->count--;
				return ref;
			}
		}

		pos = (pos + 1) % self->size;
	} while (start != pos);

	return NULL;
}

hot static inline Row*
hash_store_get(HashStore* self, Row* key, uint64_t* at)
{
	uint64_t start = row_hash(key, self->keys) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = self->rows[pos];
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
hash_store_next(HashStore* self, uint64_t* at)
{
	uint64_t pos = *at;
	for (; pos < self->size; pos++)
	{
		auto ref = self->rows[pos];
		if (!ref || ref == HT_DELETED)
			continue;
		// set to next
		*at = pos;
		return ref;
	}
	*at = UINT64_MAX;
	return NULL;
}
