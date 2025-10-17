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

typedef struct Block     Block;
typedef struct Blocks    Blocks;
typedef struct Namespace Namespace;

struct Block
{
	Stmts       stmts;
	From*       from;
	Namespace*  ns;
	Block*      parent;
	Block*      next;
};

struct Blocks
{
	Block*     list;
	Block*     list_tail;
	int        count;
	Namespace* ns;
};

static inline void
blocks_init(Blocks* self, Namespace* ns)
{
	self->list      = NULL;
	self->list_tail = NULL;
	self->count     = 0;
	self->ns        = ns;
}

static inline Block*
blocks_add(Blocks* self, Block* parent)
{
	auto block = (Block*)palloc(sizeof(Block));
	block->parent = parent;
	block->next   = NULL;
	block->ns     = self->ns;
	block->from   = NULL;
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
block_find_cte(Block* self, Str* name)
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
block_find_target(From* self, Str* name)
{
	while (self)
	{
		auto from = self;
		while (from)
		{
			auto target = from_match(from, name);
			if (target)
				return target;
			from = from->outer;
		}
		self = self->block->from;
	}
	return NULL;
}

static inline From*
block_find_from(From* self)
{
	while (self)
	{
		auto from = self;
		while (from && from_empty(from))
			from = from->outer;
		if (from)
			return from;
		self = self->block->from;
	}
	return NULL;
}

hot static inline void
block_copy_deps(Stmt* self, Block* start)
{
	// search and copy all inner blocks deps of the statements
	// of the current block stmt
	auto outer = self->block->stmts.list;
	for (; outer != self; outer = outer->next)
	{
		for (auto block = start; block; block = block->next)
		{
			auto inner = block->stmts.list;
			for (; inner; inner = inner->next)
			{
				auto dep = deps_find(&inner->deps, outer);
				if (! dep)
					continue;
				deps_add(&self->deps, dep->type, dep->stmt, dep->var);
			}
		}
	}
}
