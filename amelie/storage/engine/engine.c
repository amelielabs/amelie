
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
#include <amelie_tier>
#include <amelie_engine.h>

void
engine_init(Engine* self, Uuid* id, TierMgr* tier_mgr, Sequence* seq, bool unlogged)
{
	self->id            = *id;
	self->parts_count   = 0;
	self->volumes_count = 0;
	self->seq           = seq;
	self->unlogged      = unlogged;
	self->tier_mgr      = tier_mgr;
	part_map_init(&self->map);
	list_init(&self->parts);
	list_init(&self->volumes);
}

void
engine_free(Engine* self)
{
	part_map_free(&self->map);
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
engine_open(Engine* self, List* volumes, List* indexes)
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
}

void
engine_prepare(Engine* self, List* indexes)
{
	// todo: create initial partitions on bootstrap
	(void)self;
	(void)indexes;

	/*
	// find main volume
	Str main_name;
	str_set(&main_name, "main", 4);
	auto main = engine_find_volume(self, &main_name);
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
		engine_index_add(self, config);
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
}

void
engine_set_unlogged(Engine* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
engine_truncate(Engine* self)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
engine_index_create(Engine* self, IndexConfig* config)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
engine_index_drop(Engine* self, Str* name)
{
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

Volume*
engine_find_volume(Engine* self, Str* name)
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
engine_find(Engine* self, uint64_t id)
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
engine_iterator(Engine*      self,
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
	list_foreach(&self->parts)
	{
		auto part = list_at(Part, link);
		auto index = part_index_find(part, &config->name, true);
		it = index_iterator_merge(index, it);
	}
	iterator_open(it, key);
	return it;
}
