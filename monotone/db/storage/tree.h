#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct TreeRow TreeRow;
typedef struct Tree    Tree;

struct TreeRow
{
	RbtreeNode node;
};

struct Tree
{
	Index    index;
	Rbtree   tree;
	int      tree_count;
	Uuid*    storage;
	uint64_t lsn;
};

always_inline static inline Row*
tree_row_of(RbtreeNode* self)
{
	return row_reserved_row(self);
}

always_inline static inline Tree*
tree_of(Index* self)
{
	return (Tree*)self;
}

Index*
tree_allocate(IndexConfig*, Uuid*);
