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

Index* index_hash_allocate(IndexConfig*, Heap*);
