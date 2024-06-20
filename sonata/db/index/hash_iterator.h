#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct HashIterator HashIterator;

struct HashIterator
{
	Iterator    it;
	HttIterator iterator;
	Hash*       hash;
};

always_inline static inline HashIterator*
hash_iterator_of(Iterator* self)
{
	return (HashIterator*)self;
}

static inline bool
hash_iterator_open(Iterator* arg, RowKey* key)
{
	auto self = hash_iterator_of(arg);
	auto hash = self->hash;
	return htt_iterator_open(&self->iterator, &hash->ht, key);
}

static inline bool
hash_iterator_has(Iterator* arg)
{
	auto self = hash_iterator_of(arg);
	return htt_iterator_has(&self->iterator);
}

static inline Row*
hash_iterator_at(Iterator* arg)
{
	auto self = hash_iterator_of(arg);
	return htt_iterator_at(&self->iterator);
}

static inline void
hash_iterator_next(Iterator* arg)
{
	auto self = hash_iterator_of(arg);
	htt_iterator_next(&self->iterator);
}

static inline void
hash_iterator_close(Iterator* arg)
{
	auto self = hash_iterator_of(arg);
	htt_iterator_close(&self->iterator);
	so_free(arg);
}

static inline Iterator*
hash_iterator_allocate(Hash* hash)
{
	HashIterator* self = so_malloc(sizeof(*self));
	self->it.has   = hash_iterator_has;
	self->it.at    = hash_iterator_at;
	self->it.next  = hash_iterator_next;
	self->it.close = hash_iterator_close;
	self->hash = hash;
	htt_iterator_init(&self->iterator);
	return &self->it;
}
