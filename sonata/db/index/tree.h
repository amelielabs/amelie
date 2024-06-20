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
Row* tree_set(Tree*, Row*);
Row* tree_set_or_get(Tree*, Row*, TreePos*);
Row* tree_unset(Tree*, Row*);
Row* tree_unset_by(Tree*, TreePos*);
Row* tree_replace(Tree*, TreePos*, Row*);
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
