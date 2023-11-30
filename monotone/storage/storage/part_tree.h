#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct PartTree PartTree;

struct PartTree
{
	Rbtree tree;
};

static inline void
part_tree_init(PartTree* self)
{
	rbtree_init(&self->tree);
}

static inline Part*
part_tree_min(PartTree* self)
{
	auto min = rbtree_min(&self->tree);
	return container_of(min, Part, link_node);
}

static inline int
part_tree_compare(RbtreeNode* node, void* ptr)
{
	auto part = container_of(node, Part, link_node);
	int64_t* key = ptr;
	return part_compare(part, *key);
}

hot static inline
rbtree_get(part_tree_find, part_tree_compare(n, key))

static inline void
part_tree_add(PartTree* self, Part* part)
{
	RbtreeNode* part_ptr;
	int rc;
	rc = part_tree_find(&self->tree, NULL, &part->min, &part_ptr);
	if (rc == 0 && part_ptr)
		abort();
	rbtree_set(&self->tree, part_ptr, rc, &part->link_node);
}

static inline void
part_tree_remove(PartTree* self, Part* part)
{
	rbtree_remove(&self->tree, &part->link_node);
}

hot static inline Part*
part_tree_next(PartTree* self, Part* part)
{
	auto node = rbtree_next(&self->tree, &part->link_node);
	if (node == NULL)
		return NULL;
	return container_of(node, Part, link_node);
}

hot static inline Part*
part_tree_match(PartTree* self, int64_t key)
{
	// part[n].min >= key && key < part[n + 1].min
	RbtreeNode* part_ptr;
	int rc;
	rc = part_tree_find(&self->tree, NULL, &key, &part_ptr);
	assert(part_ptr != NULL);
	if (rc == 1)
	{
		auto prev = rbtree_prev(&self->tree, part_ptr);
		if (prev)
			part_ptr = prev;
	}
	return container_of(part_ptr, Part, link_node);
}
