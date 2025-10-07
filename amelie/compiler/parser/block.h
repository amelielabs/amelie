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
	Stmts    stmts;
	Targets* targets;
	Block*   parent;
	Block*   next;
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
	block->parent  = parent;
	block->next    = NULL;
	block->targets = NULL;
	stmts_init(&block->stmts);

	if (self->list == NULL)
		self->list = block;
	else
		self->list_tail->next = block;
	self->list_tail = block;
	self->count++;
	return block;
}

static inline Stmt*
block_find(Block* self, Str* name)
{
	for (auto block = self; block; block = block->parent)
	{
		auto stmt = stmts_find(&block->stmts, name);
		if (stmt)
			return stmt;
	}
	return NULL;
}

static inline Target*
block_target_find(Targets* self, Str* name)
{
	while (self)
	{
		auto targets = self;
		while (targets)
		{
			auto target = targets_match(targets, name);
			if (target)
				return target;
			targets = targets->outer;
		}
		self = self->block->targets;
	}
	return NULL;
}

static inline Targets*
block_targets(Targets* self)
{
	while (self)
	{
		auto targets = self;
		while (targets && targets_empty(targets))
			targets = targets->outer;
		if (targets)
			return targets;
		self = self->block->targets;
	}
	return NULL;
}
