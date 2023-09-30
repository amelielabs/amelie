#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RbtreeNode RbtreeNode;
typedef struct Rbtree     Rbtree;

struct RbtreeNode
{
	RbtreeNode* p;
	RbtreeNode* l, *r;
	char        color;
};

struct Rbtree
{
	RbtreeNode* root;
};

#define rbtree_get_def(name) \
\
extern int \
name(Rbtree*      self, \
     void*        arg, \
     void*        key, \
     RbtreeNode** match);\

#define rbtree_get(name, compare) \
\
int \
name(Rbtree*      self, \
     void*        arg, \
     void*        key, \
     RbtreeNode** match) \
{ \
	unused(arg); \
	unused(key); \
	RbtreeNode *n = self->root; \
	*match = NULL; \
	int rc = 0; \
	while (n) { \
		*match = n; \
		switch ((rc = (compare))) { \
		case  0: return 0; \
		case -1: n = n->r; \
			break; \
		case  1: n = n->l; \
			break; \
		} \
	} \
	return rc; \
}

#define rbtree_free(name, executable) \
\
static inline void \
name(RbtreeNode* n, void* arg) \
{ \
	if (n->l) \
		name(n->l, arg); \
	if (n->r) \
		name(n->r, arg); \
	executable; \
}

void rbtree_init(Rbtree*);
void rbtree_init_node(RbtreeNode*);
void rbtree_set(Rbtree*, RbtreeNode*, int, RbtreeNode*);
void rbtree_replace(Rbtree*, RbtreeNode*, RbtreeNode*);
void rbtree_remove(Rbtree*, RbtreeNode*);

RbtreeNode*
rbtree_min(Rbtree*);

RbtreeNode*
rbtree_max(Rbtree*);

RbtreeNode*
rbtree_next(Rbtree*, RbtreeNode*);

RbtreeNode*
rbtree_prev(Rbtree*, RbtreeNode*);
