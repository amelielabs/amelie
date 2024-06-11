#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Hash Hash;

struct Hash
{
	Index index;
	Htt   ht;
};

always_inline static inline Hash*
hash_of(Index* self)
{
	return (Hash*)self;
}

Index*
hash_allocate(IndexConfig*, uint64_t);
