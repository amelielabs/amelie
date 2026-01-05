
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
#include <amelie_tier.h>

void
volume_mgr_init(VolumeMgr*     self,
                TierMgr*       tier_mgr,
                Sequence*      seq,
                bool           unlogged,
                Uuid*          id,
                MappingConfig* config,
                Keys*          keys)
{
	self->id            = *id;
	self->parts_count   = 0;
	self->volumes_count = 0;
	self->seq           = seq;
	self->unlogged      = unlogged;
	self->tier_mgr      = tier_mgr;
	mapping_init(&self->mapping, config, keys);
	list_init(&self->parts);
	list_init(&self->volumes);
}

void
volume_mgr_free(VolumeMgr* self)
{
	mapping_free(&self->mapping);
	list_foreach_safe(&self->parts)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->parts_count = 0;
	list_init(&self->parts);

	list_foreach_safe(&self->volumes)
	{
		auto volume = list_at(Volume, link);
		volume_free(volume);
	}
	self->volumes_count = 0;
	list_init(&self->volumes);
}

void
volume_mgr_open(VolumeMgr* self, List* volumes, List* indexes)
{
	// prepare volumes
	list_foreach(volumes)
	{
		auto config = list_at(VolumeConfig, link);

		// match tier
		auto tier = tier_mgr_find(self->tier_mgr, &config->tier, true);

		// add volume
		auto volume = volume_allocate(config, tier);
		list_append(&self->volumes, &volume->link);
		self->volumes_count++;
	}

	// todo: recover volumes
	(void)indexes;

#if 0
	// todo: create initial partitions on bootstrap
	(void)self;
	(void)indexes;

	/*
	// find main volume
	Str main_name;
	str_set(&main_name, "main", 4);
	auto main = volume_mgr_find_volume(self, &main_name);
	(void)main;
	(void)indexes;
	*/

	/*
	// prepare partitions
	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		state_psn_follow(config->id);

		// add partition
		auto part = part_allocate(config, main->tier->config, self->seq, self->unlogged);
		list_append(&self->list_parts, &part->link);
		self->list_parts_count++;

		// add partition to the volume
		volume_add(main, part);
	}

	// recreate indexes
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		volume_mgr_index_add(self, config);
	}
	*/

	/*
	// create mapping
	list_foreach(&self->list_parts)
	{
		auto part = list_at(Part, link);
		if (! part_map_created(&self->map))
			part_map_create(&self->map);

		auto i = part->config->min;
		for (; i < part->config->max; i++)
			part_map_set(&self->map, i, part);
	}
	*/
#endif
}

void
volume_mgr_set_unlogged(VolumeMgr* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
volume_mgr_truncate(VolumeMgr* self)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
volume_mgr_index_create(VolumeMgr* self, IndexConfig* config)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
volume_mgr_index_drop(VolumeMgr* self, Str* name)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

Volume*
volume_mgr_find_volume(VolumeMgr* self, Str* name)
{
	list_foreach(&self->volumes)
	{
		auto volume = list_at(Volume, link);
		if (str_compare(&volume->config->tier, name))
			return volume;
	}
	return NULL;
}

Part*
volume_mgr_find(VolumeMgr* self, uint64_t id)
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
volume_mgr_iterator(VolumeMgr*   self,
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
		part = mapping_map(&self->mapping, key);
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
