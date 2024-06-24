#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct IndexTree IndexTree;

struct IndexTree
{
	Index index;
	Tree  tree;
};

Index* index_tree_allocate(IndexConfig*);
