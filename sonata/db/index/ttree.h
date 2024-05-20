#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct TtreePage TtreePage;
typedef struct TtreePos  TtreePos;
typedef struct Ttree     Ttree;

struct TtreePage
{
	RbtreeNode node;
	int        rows_count;
	Row*       rows[];
};

struct TtreePos
{
	TtreePage* page;
	int        page_pos;
};

struct Ttree
{
	Def*     def;
	Rbtree   tree;
	int      size_page;
	int      size_split;
	int      count_pages;
	uint64_t count;
};

void ttree_init(Ttree*, int, int, Def*);
void ttree_free(Ttree*);
Row* ttree_set(Ttree*, Row*);
Row* ttree_set_or_get(Ttree*, Row*, TtreePos*);
Row* ttree_unset(Ttree*, Row*);
Row* ttree_unset_by(Ttree*, TtreePos*);
bool ttree_seek(Ttree*, Row*, TtreePos*);
