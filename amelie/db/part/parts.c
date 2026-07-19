
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>

void
parts_init(Parts*   self,
           PartsIf* iface,
           void*    iface_arg,
           PartArg* arg,
           Keys*    keys)
{
	self->list_count = 0;
	self->arg        = arg;
	self->iface      = iface;
	self->iface_arg  = iface_arg;
	list_init(&self->list);
	part_mapping_init(&self->mapping, keys);
}

void
parts_free(Parts* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	part_mapping_free(&self->mapping);
}

void
parts_open(Parts* self, List* parts, List* indexes)
{
	list_foreach(parts)
	{
		auto config = list_at(PartConfig, link);
		state_psn_follow(config->id);

		// prepare part
		auto part = part_allocate(config, self->arg);
		parts_add(self, part);
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
			flats_add(&part->flats, flat);
		}

		// map hash partition
		part_mapping_add(&self->mapping, part);
	}

	// create pods and load heaps
	self->iface->attach(self);
}

void
parts_close(Parts* self)
{
	if (! self->list_count)
		return;

	// drop pods
	self->iface->detach(self);
}

void
parts_truncate(Parts* self)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
parts_add(Parts* self, Part* part)
{
	list_append(&self->list, &part->link);
	self->list_count++;
}

void
parts_remove(Parts* self, Part* part)
{
	list_unlink(&part->link);
	self->list_count--;
}

Part*
parts_find(Parts* self, uint64_t psn)
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
parts_index_create(Parts* self, IndexConfig* config)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_create(part, config);
	}
}

void
parts_index_remove(Parts* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

void
parts_column_create(Parts* self, Column* column)
{
	if (column->type != TYPE_VECTOR)
		return;

	// create flat stores
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		auto flat = flat_allocate(column);
		flats_add(&part->flats, flat);
	}
}

void
parts_list(Parts* self, Buf* buf, Str* ref, int flags)
{
	// show partition id on table
	if (ref)
	{
		uint64_t psn;
		if (str_u64(ref, &psn) == -1)
			error("invalid partition id");

		auto part = parts_find(self, psn);
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
