#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct IndexHash IndexHash;

struct IndexHash
{
	Index index;
	Hash  hash;
};

always_inline static inline IndexHash*
index_hash_of(Index* self)
{
	return (IndexHash*)self;
}

Index* index_hash_allocate(IndexConfig*);
