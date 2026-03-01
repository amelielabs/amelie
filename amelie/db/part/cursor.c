
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
cursor_lookup(PartMgr*     self,
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

	// no tiering (heap only)
	auto tier_mgr = self->tier_mgr;
	if (! tier_mgr_created(tier_mgr))
		return it;

	// multi-tiering
	auto branch_it = branch_iterator_allocate();
	errdefer(branch_iterator_free, branch_it);

	list_foreach(&tier_mgr->list)
	{
		auto tier = list_at(Tier, link);
		auto object = mapping_map(&tier->mapping, key);

		// iterate branches (from hot to cold)
		auto branch = object->branches;
		while (branch)
		{
			branch_iterator_reset(branch_it);
			if (branch_iterator_open(branch_it, &config->keys, branch, key))
			{
				iterator_close(it);
				return &branch_it->it;
			}
		}
	}

	// not found
	branch_iterator_free(branch_it);
	return it;
}

hot static Iterator*
cursor_scan(PartMgr*     self,
            Part*        part,
            IndexConfig* config,
            Row*         key)
{
	auto index = part_index_find(part, &config->name, true);
	auto it = index_iterator(index);
	iterator_open(it, key);

	// no tiering (heap only)
	auto tier_mgr = self->tier_mgr;
	if (! tier_mgr_created(tier_mgr))
		return it;

	// todo: different path for hash
	assert(config->type == INDEX_TREE);

	// merge heap with objects
	auto merge_it = merge_iterator_allocate(true);
	errdefer(merge_iterator_free, merge_it);
	merge_iterator_add(merge_it, it);

	list_foreach(&tier_mgr->list)
	{
		auto tier = list_at(Tier, link);

		// todo: switch to tier iterator
		// match object
		auto object = mapping_map(&tier->mapping, key);

		// add branches (from hot to cold)
		auto branch = object->branches;
		while (branch)
		{
			auto branch_it = branch_iterator_allocate();
			merge_iterator_add(merge_it, &branch_it->it);
			branch_iterator_open(branch_it, &config->keys, branch, key);
		}
	}

	merge_iterator_open(merge_it, &config->keys);
	return &merge_it->it;
}

hot static Iterator*
cursor_scan_cross(PartMgr*     self,
                  IndexConfig* config,
                  Row*         key)
{
	// prepare heap merge iterators per partition
	Iterator* it = NULL;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);

	// no tiering (heap only)
	auto tier_mgr = self->tier_mgr;
	if (! tier_mgr_created(tier_mgr))
		return it;

	// todo: different path for hash
	assert(config->type == INDEX_TREE);

	// merge all partitions heaps with objects
	auto merge_it = merge_iterator_allocate(true);
	errdefer(merge_iterator_free, merge_it);
	merge_iterator_add(merge_it, it);

	list_foreach(&tier_mgr->list)
	{
		auto tier = list_at(Tier, link);

		// todo: switch to tier iterator
		// match object
		auto object = mapping_map(&tier->mapping, key);

		// add branches (from hot to cold)
		auto branch = object->branches;
		while (branch)
		{
			auto branch_it = branch_iterator_allocate();
			merge_iterator_add(merge_it, &branch_it->it);
			branch_iterator_open(branch_it, &config->keys, branch, key);
		}
	}

	merge_iterator_open(merge_it, &config->keys);
	return &merge_it->it;
}

hot Iterator*
cursor_open(PartMgr*     self,
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
			return cursor_lookup(self, part, config, key);

		// range scan
		return cursor_scan(self, part, config, key);
	}

	// cross-partition query

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_mapping_map(&self->mapping, key);
		return cursor_lookup(self, part, config, key);
	}

	// range scan

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	return cursor_scan_cross(self, config, key);
}
