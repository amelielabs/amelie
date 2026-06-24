
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
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>

void
part_mgr_init(PartMgr*   self,
              PartMgrIf* iface,
              void*      iface_arg,
              PartArg*   arg,
              Keys*      keys)
{
	self->list_count = 0;
	self->arg        = arg;
	self->iface      = iface;
	self->iface_arg  = iface_arg;
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
part_mgr_open(PartMgr* self, List* parts, List* indexes)
{
	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		state_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config, self->arg);
		part_mgr_add(self, part);
	}

	// create indexes and stores
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		list_foreach(indexes)
		{
			auto config = list_at(IndexConfig, link);
			part_index_create(part, config);
		}

		// create flat stores
		auto primary = part_primary(part);
		list_foreach(&index_keys(primary)->columns->list)
		{
			auto column = list_at(Column, link);
			if (! column->size_flat)
				continue;
			auto flat = flat_allocate(column);
			flat_mgr_add(&part->flat_mgr, flat);
		}

		// map hash partition
		part_mapping_add(&self->mapping, part);
	}

	// create pods and load heaps
	self->iface->attach(self);
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
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (part->config->id == (int64_t)psn)
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

void
part_mgr_column_create(PartMgr* self, Column* column)
{
	if (column->type != TYPE_VECTOR)
		return;

	// create flat stores
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto flat = flat_allocate(column);
		flat_mgr_add(&part->flat_mgr, flat);
	}
}

void
part_mgr_list(PartMgr* self, Buf* buf, Str* ref, int flags)
{
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
		return;
	}

	// show partitions on table
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_status(part, buf, flags);
	}
	encode_array_end(buf);
}
