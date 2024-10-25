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
	// keys[]
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
bool tree_set(Tree*, Ref*, Ref*);
bool tree_set_or_get(Tree*, Ref*, TreePos*);
bool tree_unset(Tree*, Ref*, Ref*);
void tree_unset_by(Tree*, TreePos*);
bool tree_seek(Tree*, Ref*, TreePos*);

always_inline hot static inline Ref*
tree_at(Tree* self, TreePage* page, int pos)
{
	auto ptr = (uint8_t*)page + sizeof(TreePage) + (self->keys->key_size * pos);
	return (Ref*)ptr;
}

always_inline static inline void
tree_copy(Tree* self, TreePage* page, int pos, Ref* key)
{
	memcpy(tree_at(self, page, pos), key, self->keys->key_size);
}

always_inline static inline void
tree_copy_from(Tree* self, TreePage* page, int pos, Ref* key)
{
	memcpy(key, tree_at(self, page, pos), self->keys->key_size);
}
