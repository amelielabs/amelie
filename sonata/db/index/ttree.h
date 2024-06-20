#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct TtreePage TtreePage;
typedef struct TtreePos  TtreePos;
typedef struct Ttree     Ttree;

struct TtreePage
{
	RbtreeNode node;
	int        keys_count;
	// keys[]
};

struct TtreePos
{
	TtreePage* page;
	int        page_pos;
};

struct Ttree
{
	Keys*    keys;
	Rbtree   tree;
	int      size_page;
	int      size_split;
	int      count_pages;
	uint64_t count;
};

void ttree_init(Ttree*, int, int, Keys*);
void ttree_free(Ttree*);
Row* ttree_set(Ttree*, Row*);
Row* ttree_set_or_get(Ttree*, Row*, TtreePos*);
Row* ttree_unset(Ttree*, Row*);
Row* ttree_unset_by(Ttree*, TtreePos*);
Row* ttree_replace(Ttree*, TtreePos*, Row*);
bool ttree_seek(Ttree*, RowKey*, TtreePos*);

always_inline hot static inline RowKey*
ttree_at(Ttree* self, TtreePage* page, int pos)
{
	auto ptr = (uint8_t*)page + sizeof(TtreePage) + (self->keys->key_size * pos);
	return (RowKey*)ptr;
}

always_inline static inline void
ttree_copy(Ttree* self, TtreePage* page, int pos, RowKey* key)
{
	memcpy(ttree_at(self, page, pos), key, self->keys->key_size);
}
