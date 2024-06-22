#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
bool tree_set(Tree*, RowKey*, RowKey*);
bool tree_set_or_get(Tree*, RowKey*, TreePos*);
bool tree_unset(Tree*, RowKey*, RowKey*);
void tree_unset_by(Tree*, TreePos*);
bool tree_seek(Tree*, RowKey*, TreePos*);

always_inline hot static inline RowKey*
tree_at(Tree* self, TreePage* page, int pos)
{
	auto ptr = (uint8_t*)page + sizeof(TreePage) + (self->keys->key_size * pos);
	return (RowKey*)ptr;
}

always_inline static inline void
tree_copy(Tree* self, TreePage* page, int pos, RowKey* key)
{
	memcpy(tree_at(self, page, pos), key, self->keys->key_size);
}

always_inline static inline void
tree_copy_from(Tree* self, TreePage* page, int pos, RowKey* key)
{
	memcpy(key, tree_at(self, page, pos), self->keys->key_size);
}
