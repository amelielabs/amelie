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

typedef struct IndexTreeMerge IndexTreeMerge;

struct IndexTreeMerge
{
	Iterator      it;
	TreeIterator* current_it;
	Buf           list;
	int           list_count;
};

always_inline static inline IndexTreeMerge*
index_tree_merge_of(Iterator* self)
{
	return (IndexTreeMerge*)self;
}

hot static inline void
index_tree_merge_step(IndexTreeMerge* self)
{
	if (self->current_it)
	{
		tree_iterator_next(self->current_it);
		self->current_it = NULL;
	}

	auto list = (TreeIterator*)self->list.start;
	TreeIterator* min_iterator = NULL;
	Row*          min = NULL;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = tree_iterator_at(current);
		if (! row)
			continue;

		if (min == NULL)
		{
			min_iterator = current;
			min = row;
			continue;
		}

		auto rc = compare(current->tree->keys, min, row);
		switch (rc) {
		case 0:
			break;
		case 1:
			min_iterator = current;
			min = row;
			break;
		case -1:
			break;
		}
	}

	self->current_it = min_iterator;
}

static inline bool
index_tree_merge_open(Iterator* arg, Row* key)
{
	auto self  = index_tree_merge_of(arg);
	auto match = false;
	auto list = (TreeIterator*)self->list.start;
	for (auto i = 0; i < self->list_count; i++)
	{
		if (tree_iterator_open(&list[i], key))
			match = true;
	}
	index_tree_merge_step(self);
	return match;
}

static inline bool
index_tree_merge_has(Iterator* arg)
{
	auto self = index_tree_merge_of(arg);
	return self->current_it != NULL;
}

static inline Row*
index_tree_merge_at(Iterator* arg)
{
	auto self = index_tree_merge_of(arg);
	if (unlikely(! self->current_it))
		return NULL;
	return tree_iterator_at(self->current_it);
}

static inline void
index_tree_merge_next(Iterator* arg)
{
	auto self = index_tree_merge_of(arg);
	index_tree_merge_step(self);
}

static inline void
index_tree_merge_close(Iterator* arg)
{
	auto self = index_tree_merge_of(arg);
	buf_free(&self->list);
	am_free(arg);
}

static inline Iterator*
index_tree_merge_allocate(void)
{
	IndexTreeMerge* self = am_malloc(sizeof(*self));
	self->it.open  = index_tree_merge_open;
	self->it.has   = index_tree_merge_has;
	self->it.at    = index_tree_merge_at;
	self->it.next  = index_tree_merge_next;
	self->it.close = index_tree_merge_close;
	self->current_it = NULL;
	self->list_count = 0;
	buf_init(&self->list);
	return &self->it;
}

static inline void
index_tree_merge_add(IndexTreeMerge* self, IndexTree* index)
{
	auto it = (TreeIterator*)buf_claim(&self->list, sizeof(TreeIterator));
	tree_iterator_init(it, &index->tree);
	self->list_count++;
}
