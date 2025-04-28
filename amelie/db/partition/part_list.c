
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

void
part_list_init(PartList* self)
{
	self->unlogged   = false;
	self->list_count = 0;
	list_init(&self->list);
	part_map_init(&self->map);
}

void
part_list_free(PartList* self)
{
	// unref routes and free mapping
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
part_list_create(PartList* self,
                 bool      unlogged,
                 Sequence* seq,
                 List*     parts,
                 List*     indexes)
{
	self->unlogged = unlogged;

	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		state_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config, seq, unlogged);
		list_append(&self->list, &part->link);
		self->list_count++;
	}

	// recreate indexes
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		part_list_index_create(self, config);
	}
}

void
part_list_map(PartList* self)
{
	// create partition mapping
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (! part_map_created(&self->map))
			part_map_create(&self->map);
		int i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(&self->map, i, part);
	}
}

void
part_list_set_unlogged(PartList* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
part_list_truncate(PartList* self)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
part_list_index_create(PartList* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_create(part, config);
	}
}

void
part_list_index_drop(PartList* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, config);
	}
}

hot Part*
part_list_match(PartList* self, Core* core)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (part->core == core)
			return part;
	}
	return NULL;
}

Iterator*
part_list_iterator(PartList* self, Part* part, IndexConfig* config,
                   bool      point_lookup,
                   Row*      key)
{
	// single partition iteration
	if (part)
	{
		auto index = part_find(part, &config->name, true);
		auto it = index_iterator(index);
		iterator_open(it, key);
		return it;
	}

	// point lookup (tree or hash index)
	if (point_lookup)
	{
		part = part_map_get(&self->map, row_hash(key, &config->keys));
		auto index = part_find(part, &config->name, true);
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
		auto index = part_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);
	return it;
}
