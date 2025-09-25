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

typedef struct Block  Block;
typedef struct Blocks Blocks;

struct Block
{
	Vars   vars;
	Ctes   ctes;
	Stmts  stmts;
	Block* parent;
	Block* next;
};

struct Blocks
{
	Block* list;
	Block* list_tail;
	int    count;
};

static inline void
blocks_init(Blocks* self)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
}

static inline Block*
blocks_add(Blocks* self, Block* parent)
{
	auto block = (Block*)palloc(sizeof(Block));
	block->parent = parent;
	block->next   = NULL;
	vars_init(&block->vars);
	ctes_init(&block->ctes);
	stmts_init(&block->stmts);

	if (self->list == NULL)
		self->list = block;
	else
		self->list_tail->next = block;
	self->list_tail = block;
	self->count++;
	return block;
}

static inline Var*
block_add_var(Block* self, Str* name)
{
	for (auto block = self; block; block = block->parent)
	{
		auto var = vars_find(&block->vars, name);
		if (var)
			return var;
	}
	return vars_add(&self->vars, name);
}
