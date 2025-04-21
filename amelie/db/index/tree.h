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

typedef struct TreePage TreePage;
typedef struct TreePos  TreePos;
typedef struct Tree     Tree;

struct TreePage
{
	RbtreeNode node;
	int        keys_count;
	Row*       rows[];
};

struct TreePos
{
	TreePage* page;
	int       page_pos;
};

struct Tree
{
	Keys*    keys;
	Rbtree   tree;
	int      size_page;
	int      size_split;
	int      count_pages;
	uint64_t count;
};

void tree_init(Tree*, int, int, Keys*);
void tree_free(Tree*);
bool tree_upsert(Tree*, TreePos*, Row*);
Row* tree_replace(Tree*, Row*);
void tree_delete(Tree*, TreePos*);
Row* tree_delete_by(Tree*, Row*);
bool tree_get(Tree*, TreePos*, Row*);
