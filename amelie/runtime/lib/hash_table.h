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

typedef struct HashtableNode HashtableNode;
typedef struct Hashtable     Hashtable;

typedef bool (*HashtableCompare)(HashtableNode*, void*);

struct HashtableNode
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
hashtable_node_init(HashtableNode* self)
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
	buf_init(&self->buf);
}

static inline void
hashtable_create(Hashtable* self, int to_allocate)
{
	int size = sizeof(HashtableNode*) * to_allocate;
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
hashtable_set(Hashtable* self, HashtableNode* node)
{
	uint32_t pos = node->hash % self->size;
	auto index = (HashtableNode**)(self->buf.start);
	for (;;)
	{
		if (index[pos] == NULL)
		{
			index[pos] = node;
			node->pos = pos;
			self->count++;
			return;
		}
		pos = (pos + 1) % self->size;
		continue;
	}
}

hot static inline void
hashtable_delete(Hashtable* self, HashtableNode* node)
{
	auto index = (HashtableNode**)(self->buf.start);
	assert(index[node->pos] == node);
	index[node->pos] = NULL;
	self->count--;
}

hot static inline HashtableNode*
hashtable_get(Hashtable*       self,
              uint32_t         hash,
              HashtableCompare compare,
              void*            compare_arg)
{
	uint32_t pos = hash % self->size;
	auto index = (HashtableNode**)(self->buf.start);
	for (;;)
	{
		auto current = index[pos];
		/*
		if (current == NULL || current->hash != hash)
			break;
			*/
		/* todo: if first is deleted, second will not be found? */
		if (current == NULL)
			break;
		if (current->hash == hash && compare(current, compare_arg))
			return current;
		pos = (pos + 1) % self->size;
		continue;
	}
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

	auto index = (HashtableNode**)(self->buf.start);
	int pos = 0;
	while (pos < self->size)
	{
		if (index[pos])
			hashtable_set(&new_ht, index[pos]);
		pos++;
	}

	hashtable_free(self);
	*self = new_ht;
}

static inline HashtableNode*
hashtable_at(Hashtable* self, int pos)
{
	assert(pos < self->size);
	auto index = (HashtableNode**)(self->buf.start);
	return index[pos];
}

hot static inline int
hashtable_next(Hashtable* self, int pos)
{
	auto index = (HashtableNode**)(self->buf.start);
	for (pos++; pos < self->size; pos++)
		if (index[pos])
			return pos;
	return -1;
}
