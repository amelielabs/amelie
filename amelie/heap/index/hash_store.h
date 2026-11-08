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
	Row**       rows;
	uint64_t    count;
	uint64_t    size;
	Comparable* comparable;
};

static inline void
hash_store_init(HashStore* self)
{
	self->rows       = NULL;
	self->count      = 0;
	self->size       = 0;
	self->comparable = NULL;
}

static inline void
hash_store_free(HashStore* self)
{
	if (self->rows)
		am_free(self->rows);
	self->rows       = NULL;
	self->count      = 0;
	self->size       = 0;
	self->comparable = NULL;
}

static inline bool
hash_store_is_full(HashStore* self)
{
	return self->count > self->size / 2;
}

static inline void
hash_store_create(HashStore* self, Comparable* comparable, size_t size)
{
	size_t allocated = sizeof(Row*) * size;
	self->comparable = comparable;
	self->size       = size;
	self->rows       = am_malloc(allocated);
	memset(self->rows, 0, allocated);
}

hot static inline Row*
hash_store_set(HashStore* self, Row* key)
{
	auto     comparable  = self->comparable;
	uint64_t start       = row_hash(key, comparable) % self->size;
	uint64_t pos         = start;
	int64_t  pos_deleted = -1;
	do
	{
		auto ref = self->rows[pos];
		if (! ref)
		{
			// place key on the first deleted pos
			if (pos_deleted == -1)
				self->rows[pos] = key;
			else
				self->rows[pos_deleted] = key;
			self->count++;
			return NULL;
		}

		if (ref == HT_DELETED)
		{
			if (pos_deleted == -1)
				pos_deleted = pos;
		} else
		{
			if (! compare(comparable, ref, key))
			{
				self->rows[pos] = key;
				return ref;
			}
		}

		pos = (pos + 1) % self->size;
	} while (start != pos);

	if (pos_deleted != -1)
	{
		self->rows[pos_deleted] = key;
		self->count++;
		return NULL;
	}

	error("hashtable overflow");
	return NULL;
}

hot static inline Row*
hash_store_delete(HashStore* self, Row* key)
{
	auto     comparable = self->comparable;
	uint64_t start = row_hash(key, comparable) % self->size;
	uint64_t pos   = start;
	do
	{
		auto ref = self->rows[pos];
		if (! ref)
			break;

		if (ref != HT_DELETED)
		{
			if (! compare(comparable, ref, key))
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
	auto     comparable  = self->comparable;
	uint64_t start       = row_hash(key, comparable) % self->size;
	uint64_t pos         = start;
	int64_t  pos_deleted = -1;
	do
	{
		auto ref = self->rows[pos];
		if (! ref)
		{
			if (pos_deleted == -1)
				*at = pos;
			else
				*at = pos_deleted;
			return NULL;
		}
		if (ref == HT_DELETED)
		{
			if (pos_deleted == -1)
				pos_deleted = pos;
		} else
		{
			if (! compare(comparable, ref, key))
			{
				*at = pos;
				return ref;
			}
		}

		pos = (pos + 1) % self->size;
	} while (start != pos);

	if (pos_deleted != -1)
	{
		*at = pos_deleted;
		return NULL;
	}

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
