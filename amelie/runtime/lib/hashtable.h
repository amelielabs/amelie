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

typedef struct Hashnode  Hashnode;
typedef struct Hashtable Hashtable;

typedef bool (*HashtableCompare)(Hashnode*, void*);

#define HASHTABLE_DELETED (void*)0xffffffff

struct Hashnode
{
	uint32_t hash;
	uint32_t pos;
};

struct Hashtable
{
	Buf buf;
	int count;
	int size;
};

static inline void
hashnode_init(Hashnode* self)
{
	self->hash = 0;
	self->pos  = 0;
}

static inline void
hashtable_init(Hashtable* self)
{
	self->count = 0;
	self->size  = 0;
	buf_init(&self->buf);
}

static inline void
hashtable_free(Hashtable* self)
{
	buf_free(&self->buf);
	hashtable_init(self);
}

static inline void
hashtable_create(Hashtable* self, int to_allocate)
{
	int size = sizeof(Hashnode*) * to_allocate;
	buf_reserve(&self->buf, size);
	memset(self->buf.start, 0, size);
	buf_advance(&self->buf, size);
	self->count = 0;
	self->size  = to_allocate;
}

static inline bool
hashtable_created(Hashtable* self)
{
	return self->size > 0;
}

hot static inline void
hashtable_set(Hashtable* self, Hashnode* node)
{
	uint32_t start = node->hash % self->size;
	uint32_t pos   = start;

	auto index = (Hashnode**)(self->buf.start);
	do
	{
		if (!index[pos] || index[pos] == HASHTABLE_DELETED)
		{
			index[pos] = node;
			node->pos = pos;
			self->count++;
			return;
		}
		pos = (pos + 1) % self->size;
		continue;

	} while (start != pos);

	error("hashtable overflow");
}

hot static inline void
hashtable_delete(Hashtable* self, Hashnode* node)
{
	auto index = (Hashnode**)(self->buf.start);
	assert(index[node->pos] == node);
	index[node->pos] = HASHTABLE_DELETED;
	self->count--;
}

hot static inline Hashnode*
hashtable_get(Hashtable*       self,
              uint32_t         hash,
              HashtableCompare compare,
              void*            compare_arg)
{
	if (unlikely(! self->count))
		return NULL;

	uint32_t start = hash % self->size;
	uint32_t pos   = start;

	auto index = (Hashnode**)(self->buf.start);
	do
	{
		auto current = index[pos];
		if (! current)
			return NULL;

		if (current != HASHTABLE_DELETED)
			if (current->hash == hash && compare(current, compare_arg))
				return current;

		pos = (pos + 1) % self->size;
	} while (start != pos);

	error("hashtable overflow");
	return NULL;
}

static inline bool
hashtable_is_full(Hashtable* self)
{
	return self->count > (self->size / 2);
}

hot static inline void
hashtable_reserve(Hashtable* self)
{
	if (likely(! hashtable_is_full(self)))
		return;

	Hashtable new_ht;
	hashtable_init(&new_ht);
	hashtable_create(&new_ht, self->size * 2);

	auto index = (Hashnode**)(self->buf.start);
	int pos = 0;
	while (pos < self->size)
	{
		if (index[pos] && index[pos] != HASHTABLE_DELETED)
			hashtable_set(&new_ht, index[pos]);
		pos++;
	}

	hashtable_free(self);
	*self = new_ht;
}

static inline Hashnode*
hashtable_at(Hashtable* self, int pos)
{
	assert(pos < self->size);
	auto index = (Hashnode**)(self->buf.start);
	return index[pos];
}

hot static inline int
hashtable_next(Hashtable* self, int pos)
{
	auto index = (Hashnode**)(self->buf.start);
	for (pos++; pos < self->size; pos++)
		if (index[pos] && index[pos] != HASHTABLE_DELETED)
			return pos;
	return -1;
}
