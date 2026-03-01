
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

void
part_mgr_init(PartMgr*       self,
              PartMgrIf*     iface,
              void*          iface_arg,
              PartMgrConfig* config,
              PartArg*       arg,
              TierMgr*       tier_mgr,
              Keys*          keys)
{
	self->list_count = 0;
	self->config     = config;
	self->arg        = arg;
	self->iface      = iface;
	self->iface_arg  = iface_arg;
	self->tier_mgr   = tier_mgr;
	list_init(&self->list);
	part_mapping_init(&self->mapping, keys);
}

void
part_mgr_free(PartMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	part_mapping_free(&self->mapping);
}

void
part_mgr_open(PartMgr* self, List* indexes)
{
	// recover files
	part_mgr_recover(self);

	// create indexes
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		list_foreach(indexes)
		{
			auto config = list_at(IndexConfig, link);
			part_index_create(part, config);
		}
	}

	// create pods and load heaps
	self->iface->attach(self);

	// map hash partitions
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_mapping_add(&self->mapping, part);

		// update metrics
		auto lsn = part->heap->header->lsn;
		state_lsn_follow(lsn);
		track_lsn_set(&part->track, lsn);
	}
}

void
part_mgr_close(PartMgr* self)
{
	if (! self->list_count)
		return;

	// drop pods
	self->iface->detach(self);
}

void
part_mgr_drop(PartMgr* self)
{
	if (! self->list_count)
		return;

	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_mgr_remove(self, part);
		id_delete(&part->id, STATE_COMPLETE);
		part_free(part);
	}
	assert(! self->list_count);
	list_init(&self->list);

	// delete volumes
	volume_mgr_rmdir(&self->config->volumes);
	volume_mgr_unref(&self->config->volumes);
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
part_mgr_add(PartMgr* self, Part* part)
{
	list_append(&self->list, &part->link);
	self->list_count++;
	volume_pin(part->id.volume);
}

void
part_mgr_remove(PartMgr* self, Part* part)
{
	list_unlink(&part->link);
	self->list_count--;
}

Part*
part_mgr_find(PartMgr* self, uint64_t psn)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		if (part->id.id == psn)
			return part;
	}
	return NULL;
}

void
part_mgr_index_create(PartMgr* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_create(part, config);
	}
}

void
part_mgr_index_remove(PartMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

Buf*
part_mgr_list(PartMgr* self, Str* ref, int flags)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show partition id on table
	if (ref)
	{
		int64_t psn;
		if (str_toint(ref, &psn) == -1)
			error("invalid partition id");

		auto part = part_mgr_find(self, psn);
		if (! part)
			encode_null(buf);
		else
			part_status(part, buf, flags);
		return buf;
	}

	// show partitions on table
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_status(part, buf, flags);
	}
	encode_array_end(buf);
	return buf;
}
