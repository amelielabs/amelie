
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>

void
part_list_init(PartList* self, PartMgr* mgr)
{
	self->reference  = false;
	self->list_count = 0;
	self->mgr        = mgr;
	list_init(&self->list);
	part_map_init(&self->map);
}

void
part_list_free(PartList* self)
{
	// unref routes
	part_map_unmap(&self->map);
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
part_list_create(PartList* self, bool reference,
                 List*     parts,
                 List*     indexes)
{
	self->reference = reference;

	// prepare partition mapping
	part_map_create(&self->map);

	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		config_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config);
		list_append(&self->list, &part->link);
		self->list_count++;

		// register and map partition
		part_mgr_add(self->mgr, &self->map, part);
	}

	// recreate indexes
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		part_list_create_index(self, config);
	}
}

hot Part*
part_list_match(PartList* self, Uuid* node)
{
	// get first part, if reference or find part by node id
	if (self->reference)
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

void
part_list_create_index(PartList* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_create_index(part, config);
	}
}

void
part_list_drop_index(PartList* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_drop_index(part, config);
	}
}
