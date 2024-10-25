
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>

void
rbtree_init(Rbtree* self)
{
	self->root = NULL;
}

void
rbtree_init_node(RbtreeNode* self)
{
	self->color = 2;
	self->p = NULL;
	self->l = NULL;
	self->r = NULL;
}

#define RBTREE_BLACK 0
#define RBTREE_RED   1
#define RBTREE_UNDEF 2

hot static inline void
rotate_left(Rbtree* self, RbtreeNode* n)
{
	RbtreeNode* p = n;
	RbtreeNode* q = n->r;
	RbtreeNode* parent = n->p;
	if (likely(p->p != NULL)) {
		if (parent->l == p)
			parent->l = q;
		else
			parent->r = q;
	} else {
		self->root = q;
	}
	q->p = parent;
	p->p = q;
	p->r = q->l;
	if (p->r)
		p->r->p = p;
	q->l = p;
}

hot static inline void
rotate_right(Rbtree* self, RbtreeNode* n)
{
	RbtreeNode* p = n;
	RbtreeNode* q = n->l;
	RbtreeNode* parent = n->p;
	if (likely(p->p != NULL)) {
		if (parent->l == p)
			parent->l = q;
		else
			parent->r = q;
	} else {
		self->root = q;
	}
	q->p = parent;
	p->p = q;
	p->l = q->r;
	if (p->l)
		p->l->p = p;
	q->r = p;
}

hot static inline void
set_fixup(Rbtree* self, RbtreeNode* n)
{
	RbtreeNode* p;
	while ((p = n->p) && (p->color == RBTREE_RED))
	{
		RbtreeNode* g = p->p;
		if (p == g->l) {
			RbtreeNode* u = g->r;
			if (u && u->color == RBTREE_RED) {
				g->color = RBTREE_RED;
				p->color = RBTREE_BLACK;
				u->color = RBTREE_BLACK;
				n = g;
			} else {
				if (n == p->r) {
					rotate_left(self, p);
					n = p;
					p = n->p;
				}
				g->color = RBTREE_RED;
				p->color = RBTREE_BLACK;
				rotate_right(self, g);
			}
		} else {
			RbtreeNode* u = g->l;
			if (u && u->color == RBTREE_RED) {
				g->color = RBTREE_RED;
				p->color = RBTREE_BLACK;
				u->color = RBTREE_BLACK;
				n = g;
			} else {
				if (n == p->l) {
					rotate_right(self, p);
					n = p;
					p = n->p;
				}
				g->color = RBTREE_RED;
				p->color = RBTREE_BLACK;
				rotate_left(self, g);
			}
		}
	}
	self->root->color = RBTREE_BLACK;
}

hot void
rbtree_set(Rbtree* self, RbtreeNode* p, int prel, RbtreeNode* n)
{
	n->color = RBTREE_RED;
	n->p     = p;
	n->l     = NULL;
	n->r     = NULL;
	if (likely(p)) {
		assert(prel != 0);
		if (prel > 0)
			p->l = n;
		else
			p->r = n;
	} else {
		self->root = n;
	}
	set_fixup(self, n);
}

void
rbtree_replace(Rbtree* self, RbtreeNode* o, RbtreeNode* n)
{
	RbtreeNode* p = o->p;
	if (p) {
		if (p->l == o) {
			p->l = n;
		} else {
			p->r = n;
		}
	} else {
		self->root = n;
	}
	if (o->l)
		o->l->p = n;
	if (o->r)
		o->r->p = n;
	*n = *o;
}

hot void
rbtree_remove(Rbtree* self, RbtreeNode* n)
{
	if (unlikely(n->color == RBTREE_UNDEF))
		return;
	RbtreeNode* l = n->l;
	RbtreeNode* r = n->r;
	RbtreeNode* x = NULL;
	if (l == NULL) {
		x = r;
	} else
	if (r == NULL) {
		x = l;
	} else {
		x = r;
		while (x->l)
			x = x->l;
	}
	RbtreeNode* p = n->p;
	if (p) {
		if (p->l == n) {
			p->l = x;
		} else {
			p->r = x;
		}
	} else {
		self->root = x;
	}
	char color;
	if (l && r) {
		color    = x->color;
		x->color = n->color;
		x->l     = l;
		l->p     = x;
		if (x != r) {
			p    = x->p;
			x->p = n->p;
			n    = x->r;
			p->l = n;
			x->r = r;
			r->p = x;
		} else {
			x->p = p;
			p    = x;
			n    = x->r;
		}
	} else {
		color = n->color;
		n     = x;
	}
	if (n)
		n->p = p;

	if (color == RBTREE_RED)
		return;
	if (n && n->color == RBTREE_RED) {
		n->color = RBTREE_BLACK;
		return;
	}

	RbtreeNode* s;
	do {
		if (unlikely(n == self->root))
			break;

		if (n == p->l) {
			s = p->r;
			if (s->color == RBTREE_RED)
			{
				s->color = RBTREE_BLACK;
				p->color = RBTREE_RED;
				rotate_left(self, p);
				s = p->r;
			}
			if ((!s->l || (s->l->color == RBTREE_BLACK)) &&
			    (!s->r || (s->r->color == RBTREE_BLACK)))
			{
				s->color = RBTREE_RED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->r || (s->r->color == RBTREE_BLACK)))
			{
				s->l->color = RBTREE_BLACK;
				s->color    = RBTREE_RED;
				rotate_right(self, s);
				s = p->r;
			}
			s->color    = p->color;
			p->color    = RBTREE_BLACK;
			s->r->color = RBTREE_BLACK;
			rotate_left(self, p);
			n = self->root;
			break;
		} else {
			s = p->l;
			if (s->color == RBTREE_RED)
			{
				s->color = RBTREE_BLACK;
				p->color = RBTREE_RED;
				rotate_right(self, p);
				s = p->l;
			}
			if ((!s->l || (s->l->color == RBTREE_BLACK)) &&
				(!s->r || (s->r->color == RBTREE_BLACK)))
			{
				s->color = RBTREE_RED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->l || (s->l->color == RBTREE_BLACK)))
			{
				s->r->color = RBTREE_BLACK;
				s->color    = RBTREE_RED;
				rotate_left(self, s);
				s = p->l;
			}
			s->color    = p->color;
			p->color    = RBTREE_BLACK;
			s->l->color = RBTREE_BLACK;
			rotate_right(self, p);
			n = self->root;
			break;
		}
	} while (n->color == RBTREE_BLACK);
	if (n)
		n->color = RBTREE_BLACK;
}

RbtreeNode*
rbtree_min(Rbtree* self)
{
	RbtreeNode* n = self->root;
	if (unlikely(n == NULL))
		return NULL;
	while (n->l)
		n = n->l;
	return n;
}

RbtreeNode*
rbtree_max(Rbtree* self)
{
	RbtreeNode* n = self->root;
	if (unlikely(n == NULL))
		return NULL;
	while (n->r)
		n = n->r;
	return n;
}

RbtreeNode*
rbtree_next(Rbtree* self, RbtreeNode* n)
{
	if (unlikely(n == NULL))
		return rbtree_min(self);
	if (n->r) {
		n = n->r;
		while (n->l)
			n = n->l;
		return n;
	}
	RbtreeNode* p;
	while ((p = n->p) && p->r == n)
		n = p;
	return p;
}

RbtreeNode*
rbtree_prev(Rbtree* self, RbtreeNode* n)
{
	if (unlikely(n == NULL))
		return rbtree_max(self);
	if (n->l) {
		n = n->l;
		while (n->r)
			n = n->r;
		return n;
	}
	RbtreeNode* p;
	while ((p = n->p) && p->l == n)
		n = p;
	return p;
}
