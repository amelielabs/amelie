
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>

typedef struct
{
	Value*  values;
	Value*  refs;
	int64_t identity;
} RowValues;

hot always_inline static inline int
row_map_compare(MappingRange* self, Part* part, RowValues* key)
{
	return value_compare_row_refs(self->keys, object_min(part->object),
	                              key->values,
	                              key->refs,
	                              key->identity);
}

hot static inline
rbtree_get(row_map_find, row_map_compare(arg, mapping_range_part(n), key))

Part*
row_map(Table* table, Value* refs, Value* values, int64_t identity)
{
	// values are row columns
	auto mapping = &table->volume_mgr.mapping;
	RowValues self =
	{
		.values   = values,
		.refs     = refs,
		.identity = identity
	};

	// hash partitioning
	auto hash_partition = value_hash_row(mapping->keys, refs, values, identity);
	hash_partition %= MAPPING_MAX;
	auto range = mapping->map[hash_partition];
	assert(range);

	// range partitioning
	RbtreeNode* node = NULL;
	int rc = row_map_find(&range->tree, &self, &self, &node);
	assert(node != NULL);
	if (rc > 0) {
		auto prev = rbtree_prev(&range->tree, node);
		if (prev)
			node = prev;
	}
	return mapping_range_part(node);
}

hot always_inline static inline int
row_map_keys_compare(MappingRange* self, Part* part, RowValues* key)
{
	return value_compare_keys(self->keys, object_min(part->object),
	                          key->values);
}

hot static inline
rbtree_get(row_map_keys_find, row_map_keys_compare(arg, mapping_range_part(n), key))

Part*
row_map_keys(Table* table, Value* values)
{
	// values are row keys
	auto mapping = &table->volume_mgr.mapping;
	RowValues self =
	{
		.values   = values,
		.refs     = NULL,
		.identity = 0
	};

	// hash partitioning
	auto hash_partition = value_hash_keys(mapping->keys, NULL, values, 0);
	hash_partition %= MAPPING_MAX;
	auto range = mapping->map[hash_partition];
	assert(range);

	// range partitioning
	RbtreeNode* node = NULL;
	int rc = row_map_keys_find(&range->tree, &self, &self, &node);
	assert(node != NULL);
	if (rc > 0) {
		auto prev = rbtree_prev(&range->tree, node);
		if (prev)
			node = prev;
	}
	return mapping_range_part(node);
}
