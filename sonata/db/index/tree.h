#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Tree Tree;

struct Tree
{
	Index index;
	Ttree tree;
};

always_inline static inline Tree*
tree_of(Index* self)
{
	return (Tree*)self;
}

Index*
tree_allocate(IndexConfig*, uint64_t);
