
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
#include <amelie_heap.h>
#include <amelie_object.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

void
part_mgr_init(PartMgr* self)
{
	self->unlogged   = false;
	self->list_count = 0;
	list_init(&self->list);
	part_map_init(&self->map);
}

void
part_mgr_free(PartMgr* self)
{
	part_map_free(&self->map);
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
part_mgr_create(PartMgr*  self,
                Source*   source,
                Sequence* seq,
                bool      unlogged,
                List*     parts,
                List*     indexes)
{
	self->seq      = seq;
	self->unlogged = unlogged;

	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		state_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config, source, seq, unlogged);
		list_append(&self->list, &part->link);
		self->list_count++;
	}

	// recreate indexes
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		part_mgr_index_create(self, config);
	}
}

void
part_mgr_map(PartMgr* self)
{
	// create partition mapping
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (! part_map_created(&self->map))
			part_map_create(&self->map);

		auto i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(&self->map, i, part);
	}
}

void
part_mgr_set_unlogged(PartMgr* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
part_mgr_truncate(PartMgr* self)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
part_mgr_index_create(PartMgr* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
part_mgr_index_drop(PartMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

Part*
part_mgr_find(PartMgr* self, uint64_t id)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (part->config->id == (int64_t)id)
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
		part = part_map_get(&self->map, row_hash(key, &config->keys));
		auto index = part_index_find(part, &config->name, true);
		auto it = index_iterator(index);
		iterator_open(it, key);
		return it;
	}

	// merge all hash partitions (without key)
	// merge all tree partitions (without key, ordered)
	// merge all tree partitions (with key, ordered)
	Iterator* it = NULL;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);
	return it;
}
