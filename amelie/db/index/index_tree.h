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

typedef struct IndexTree IndexTree;

struct IndexTree
{
	Index index;
	Tree  tree;
};

Index* index_tree_allocate(IndexConfig*, Heap*);
