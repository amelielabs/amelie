
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
part_mgr_init(PartMgr* self)
{
	self->reference  = false;
	self->list_count = 0;
	list_init(&self->list);
	part_map_init(&self->map);
}

void
part_mgr_free(PartMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->list_count = 0;
	list_init(&self->list);
	part_map_free(&self->map);
}

void
part_mgr_open(PartMgr* self, bool reference, List* parts, List* indexes)
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
	}
}

hot Part*
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

hot Part*
part_mgr_match(PartMgr* self, Uuid* node)
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
