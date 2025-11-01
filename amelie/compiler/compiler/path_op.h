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

typedef struct PathOp  PathOp;
typedef struct PathOps PathOps;

struct PathOp
{
	Ast*    op;
	int     names_start;
	int     names_end;
	PathOp* next;
};

struct PathOps
{
	PathOp*  list;
	PathOp*  list_tail;
	AstList* names;
};

static inline PathOp*
path_op_allocate(Ast* op, int start, int end)
{
	auto self = (PathOp*)palloc(sizeof(PathOp));
	self->op          = op;
	self->names_start = start;
	self->names_end   = end;
	self->next        = NULL;
	return self;
}

static inline void
path_ops_init(PathOps* self, AstList* names)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->names     = names;
}

static inline void
path_ops_add(PathOps* self, Ast* expr, int start, int end)
{
	switch (expr->id) {
	case KAND:
	{
		// [start, and->stop]
		auto and_end = expr->integer;
		path_ops_add(self, expr->l, start, and_end);
		path_ops_add(self, expr->r, and_end, end);
		break;
	}
	case KOR:
	{
		// skipped
		break;
	}
	case KGTE:
	case KLTE:
	case '>':
	case '<':
	case '=':
	{
		// [target.]name = value | [target.]name
		auto op = path_op_allocate(expr, start, end);
		if (self->list == NULL)
			self->list = op;
		else
			self->list_tail->next = op;
		self->list_tail = op;
		break;
	}
	case KBETWEEN:
	{
		// NOT BETWEEN
		if (! expr->integer)
			break;

		auto x = expr->r->l;
		auto y = expr->r->r;

		// expr >= x AND expr <= y
		auto gte = ast(KGTE);
		gte->l = expr->l;
		gte->r = x;

		auto lte = ast(KLTE);
		lte->l = expr->l;
		lte->r = y;

		auto and = ast(KAND);
		and->l = gte;
		and->r = lte;
		and->integer = expr->r->integer;
		path_ops_add(self, and, start, end);
	}
	default:
		break;
	}
}

static inline void
path_ops_create(PathOps* self, Ast* expr)
{
	auto start = 0;
	auto end   = 0;
	if (self->names)
		end = self->names->count;
	path_ops_add(self, expr, start, end);
}
