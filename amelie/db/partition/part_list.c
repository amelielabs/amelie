
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

void
part_list_init(PartList* self, PartMgr* mgr)
{
	self->shared     = false;
	self->unlogged   = false;
	self->list_count = 0;
	self->mgr        = mgr;
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
		// unregister partition
		part_mgr_del(self->mgr, part);
		part_free(part);
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
part_list_create(PartList* self,
                 bool      shared,
                 bool      unlogged,
                 Sequence* seq,
                 List*     parts,
                 List*     indexes)
{
	self->shared = shared;
	self->unlogged = unlogged;

	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		config_psn_follow(config->id);

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
	// register and create partitions mappings
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_mgr_add(self->mgr, &self->map, part);
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
part_list_match(PartList* self, Uuid* node)
{
	// get first part if shared or find by node id
	if (self->shared)
	{
		auto first = list_first(&self->list);
		return container_of(first, Part, link);
	}
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (! uuid_compare(&part->config->node, node))
			continue;
		return part;
	}
	return NULL;
}
