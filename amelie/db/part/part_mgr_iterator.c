
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>

hot static Iterator*
part_mgr_iterator_lookup(PartMgr*     self,
                         Part*        part,
                         IndexConfig* config,
                         Row*         key)
{
	auto index = part_index_find(part, &config->name, true);

	// check heap first
	auto it = index_iterator(index);
	errdefer(iterator_close, it);
	if (iterator_open(it, key))
		return it;

	// single tier (heap only)
	auto tier_mgr = self->tier_mgr;
	if (tier_mgr_empty(tier_mgr))
		return it;

	// multi-tiering
	auto obj_it = object_iterator_allocate();
	errdefer(iterator_close, obj_it);

	// check pending objects
	auto tier = tier_mgr_first(tier_mgr);
	list_foreach(&tier->list_pending)
	{
		auto obj = list_at(Object, id.link);
		object_iterator_reset(obj_it);
		if (object_iterator_open(obj_it, &config->keys, obj, key))
		{
			iterator_close(it);
			return &obj_it->it;
		}
	}

	// todo: other tiers (include object mapping)

	object_iterator_free(obj_it);
	return it;
}

hot static Iterator*
part_mgr_iterator_range(PartMgr*     self,
                        Part*        part,
                        IndexConfig* config,
                        Row*         key)
{
	auto index = part_index_find(part, &config->name, true);
	auto it = index_iterator(index);
	iterator_open(it, key);

	// single tier (heap only)
	auto tier_mgr = self->tier_mgr;
	if (tier_mgr_empty(tier_mgr))
		return it;

	// todo: different path for hash
	assert(config->type == INDEX_TREE);

	// merge heap with pending objects
	auto merge_it = merge_iterator_allocate(true);
	errdefer(merge_iterator_free, merge_it);
	merge_iterator_add(merge_it, it);

	auto tier = tier_mgr_first(tier_mgr);
	list_foreach(&tier->list_pending)
	{
		auto obj = list_at(Object, id.link);
		auto obj_it = object_iterator_allocate();
		merge_iterator_add(merge_it, &obj_it->it);
		object_iterator_open(obj_it, &config->keys, obj, key);
	}

	// todo: other tiers (include object mapping)

	merge_iterator_open(merge_it, &config->keys);
	return &merge_it->it;
}

hot static Iterator*
part_mgr_iterator_range_cross(PartMgr*     self,
                              IndexConfig* config,
                              Row*         key)
{
	// prepare heap merge iterators per partition
	Iterator* it = NULL;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, id.link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);

	// single tier (heap only)
	auto tier_mgr = self->tier_mgr;
	if (tier_mgr_empty(tier_mgr))
		return it;

	// todo: different path for hash
	assert(config->type == INDEX_TREE);

	// merge all partitions heaps with pending objects
	auto merge_it = merge_iterator_allocate(true);
	errdefer(merge_iterator_free, merge_it);
	merge_iterator_add(merge_it, it);

	auto tier = tier_mgr_first(tier_mgr);
	list_foreach(&tier->list_pending)
	{
		auto obj = list_at(Object, id.link);
		auto obj_it = object_iterator_allocate();
		merge_iterator_add(merge_it, &obj_it->it);
		object_iterator_open(obj_it, &config->keys, obj, key);
	}

	// todo: other tiers (include object mapping)

	merge_iterator_open(merge_it, &config->keys);
	return &merge_it->it;
}

hot Iterator*
part_mgr_iterator(PartMgr*     self,
                  Part*        part,
                  IndexConfig* config,
                  bool         point_lookup,
                  Row*         key)
{
	// partition query
	if (part)
	{
		// point lookup
		if (point_lookup)
			return part_mgr_iterator_lookup(self, part, config, key);

		// range scan
		return part_mgr_iterator_range(self, part, config, key);
	}

	// cross-partition query

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_mapping_map(&self->mapping, key);
		return part_mgr_iterator_lookup(self, part, config, key);
	}

	// range scan

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	return part_mgr_iterator_range_cross(self, config, key);
}
