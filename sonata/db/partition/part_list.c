
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
#include <sonata_def.h>
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

	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		config_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config);
		list_append(&self->list, &part->link);
		self->list_count++;

		// prepare indexes
		part_open(part, indexes);

		// register partition
		part_mgr_add(self->mgr, part);
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
