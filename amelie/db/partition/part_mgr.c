
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_partition.h>

void
part_mgr_init(PartMgr*  self,
              TierMgr*  tier_mgr,
              Sequence* seq,
              bool      unlogged,
              Keys*     keys)
{
	self->parts_count = 0;
	self->seq         = seq;
	self->unlogged    = unlogged;
	self->tier_mgr    = tier_mgr;
	part_mapping_init(&self->mapping, keys);
	list_init(&self->parts);
}

void
part_mgr_free(PartMgr* self)
{
	list_foreach_safe(&self->parts)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->parts_count = 0;
	list_init(&self->parts);
	part_mapping_free(&self->mapping);
}

void
part_mgr_open(PartMgr* self, List* indexes)
{
	// read partitions
	part_mgr_recover(self, indexes);
}

void
part_mgr_add(PartMgr* self, Part* part)
{
	list_init(&part->link);
	list_append(&self->parts, &part->link);
	self->parts_count++;
}

void
part_mgr_remove(PartMgr* self, Part* part)
{
	list_unlink(&part->link);
	self->parts_count--;
}

void
part_mgr_truncate(PartMgr* self)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
part_mgr_index_add(PartMgr* self, IndexConfig* config)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
part_mgr_index_remove(PartMgr* self, Str* name)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

void
part_mgr_set_unlogged(PartMgr* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

Part*
part_mgr_find(PartMgr* self, uint64_t id)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		if (part->id.id == id)
			return part;
	}
	return NULL;
}

Iterator*
part_mgr_iterator(PartMgr*     self,
                  Part*        part,
                  IndexConfig* config,
                  bool         point_lookup,
                  Row*         key)
{
	// single partition iteration
	if (part)
	{
		auto index = part_index_find(part, &config->name, true);
		auto it = index_iterator(index);
		iterator_open(it, key);
		return it;
	}

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_mapping_map(&self->mapping, key);
		auto index = part_index_find(part, &config->name, true);
		auto it = index_iterator(index);
		iterator_open(it, key);
		return it;
	}

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	Iterator* it = NULL;
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);
	return it;
}
