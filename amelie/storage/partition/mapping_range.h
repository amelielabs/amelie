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

typedef struct MappingRange MappingRange;

struct MappingRange
{
	Rbtree tree;
	int    tree_count;
	Keys*  keys;
	List   link;
};

static inline MappingRange*
mapping_range_allocate(Keys* keys)
{
	auto self = (MappingRange*)am_malloc(sizeof(MappingRange));
	self->keys       = keys;
	self->tree_count = 0;
	rbtree_init(&self->tree);
	list_init(&self->link);
	return self;
}

static inline void
mapping_range_free(MappingRange* self)
{
	am_free(self);
}

always_inline static inline Part*
mapping_range_part(RbtreeNode* node)
{
	return container_of(node, Part, link_range);
}

static inline Part*
mapping_range_min(MappingRange* self)
{
	if (self->tree_count == 0)
		return NULL;
	auto min = rbtree_min(&self->tree);
	return mapping_range_part(min);
}

static inline Part*
mapping_range_max(MappingRange* self)
{
	if (self->tree_count == 0)
		return NULL;
	auto max = rbtree_max(&self->tree);
	return mapping_range_part(max);
}

hot always_inline static inline int
mapping_range_compare(MappingRange* self, Part* part, Row* key)
{
	if (self->tree_count == 1)
		return 0;
	return compare(self->keys, object_min(part->object), key);
}

hot static inline
rbtree_get(mapping_range_find, mapping_range_compare(arg, mapping_range_part(n), key))

static inline void
mapping_range_add(MappingRange* self, Part* part)
{
	const auto key = object_min(part->object);
	RbtreeNode* node;
	int rc;
	rc = mapping_range_find(&self->tree, NULL, key, &node);
	if (rc == 0 && node)
		assert(0);
	rbtree_set(&self->tree, node, rc, &part->link_range);
	self->tree_count++;
}

static inline void
mapping_range_remove(MappingRange* self, Part* part)
{
	rbtree_remove(&self->tree, &part->link_range);
	self->tree_count--;
	assert(self->tree_count >= 0);
}

static inline void
mapping_range_replace(MappingRange* self, Part* a, Part* b)
{
	rbtree_replace(&self->tree, &a->link_range, &b->link_range);
}

static inline Part*
mapping_range_next(MappingRange* self, Part* part)
{
	auto node = rbtree_next(&self->tree, &part->link_range);
	if (! node)
		return NULL;
	return mapping_range_part(node);
}

static inline Part*
mapping_range_prev(MappingRange* self, Part* part)
{
	auto node = rbtree_prev(&self->tree, &part->link_range);
	if (! node)
		return NULL;
	return mapping_range_part(node);
}

hot static inline Part*
mapping_range_map(MappingRange* self, Row* key)
{
	// part[n].min >= key && key < part[n + 1].min
	RbtreeNode* node = NULL;
	int rc = mapping_range_find(&self->tree, self, key, &node);
	assert(node != NULL);
	if (rc > 0) {
		auto prev = rbtree_prev(&self->tree, node);
		if (prev)
			node = prev;
	}
	return mapping_range_part(node);
}

hot static inline Part*
mapping_range_gte(MappingRange* self, Row* key)
{
	if (unlikely(self->tree_count == 0))
		return NULL;
	auto part = mapping_range_map(self, key);
	if (compare(self->keys, object_max(part->object), key) > 0)
		return mapping_range_next(self, part);
	return part;
}
