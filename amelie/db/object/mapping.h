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

typedef struct Mapping Mapping;

struct Mapping
{
	Rbtree tree;
	int    tree_count;
	Keys*  keys;
};

static inline void
mapping_init(Mapping* self, Keys* keys)
{
	self->keys       = keys;
	self->tree_count = 0;
	rbtree_init(&self->tree);
}

always_inline static inline Object*
mapping_of(RbtreeNode* node)
{
	return container_of(node, Object, link_mapping);
}

static inline Object*
mapping_min(Mapping* self)
{
	if (! self->tree_count)
		return NULL;
	auto min = rbtree_min(&self->tree);
	return mapping_of(min);
}

static inline Object*
mapping_max(Mapping* self)
{
	if (! self->tree_count)
		return NULL;
	auto max = rbtree_max(&self->tree);
	return mapping_of(max);
}

hot always_inline static inline int
mapping_compare(Mapping* self, Object* object, Row* key)
{
	if (self->tree_count == 1)
		return 0;
	return compare(self->keys, branch_min(object->root), key);
}

hot static inline
rbtree_get(mapping_find, mapping_compare(arg, mapping_of(n), key))

static inline void
mapping_add(Mapping* self, Object* object)
{
	const auto key = branch_min(object->root);
	RbtreeNode* node;
	auto rc = mapping_find(&self->tree, NULL, key, &node);
	if (rc == 0 && node)
		assert(0);
	rbtree_set(&self->tree, node, rc, &object->link_mapping);
	self->tree_count++;
}

static inline void
mapping_remove(Mapping* self, Object* object)
{
	rbtree_remove(&self->tree, &object->link_mapping);
	self->tree_count--;
	assert(self->tree_count >= 0);
}

static inline void
mapping_replace(Mapping* self, Object* a, Object* b)
{
	rbtree_replace(&self->tree, &a->link_mapping, &b->link_mapping);
}

static inline Object*
mapping_next(Mapping* self, Object* object)
{
	auto node = rbtree_next(&self->tree, &object->link_mapping);
	if (! node)
		return NULL;
	return mapping_of(node);
}

static inline Object*
mapping_prev(Mapping* self, Object* object)
{
	auto node = rbtree_prev(&self->tree, &object->link_mapping);
	if (! node)
		return NULL;
	return mapping_of(node);
}

hot static inline Object*
mapping_map(Mapping* self, Row* key)
{
	// part[n].min >= key && key < part[n + 1].min
	RbtreeNode* node = NULL;
	int rc = mapping_find(&self->tree, self, key, &node);
	assert(node != NULL);
	if (rc > 0) {
		auto prev = rbtree_prev(&self->tree, node);
		if (prev)
			node = prev;
	}
	return mapping_of(node);
}

hot static inline Object*
mapping_gte(Mapping* self, Row* key)
{
	if (unlikely(! self->tree_count))
		return NULL;
	auto object = mapping_map(self, key);
	if (compare(self->keys, branch_max(object->root), key) > 0)
		return mapping_next(self, object);
	return object;
}
