
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>

void
parts_init(Parts*   self,
           PartsIf* iface,
           void*    iface_arg,
           PartArg* arg)
{
	self->part      = NULL;
	self->arg       = arg;
	self->iface     = iface;
	self->iface_arg = iface_arg;
}

void
parts_free(Parts* self)
{
	if (self->part)
	{
		part_free(self->part);
		self->part = NULL;
	}
}

void
parts_open(Parts* self, List* indexes)
{
	// prepare partition
	auto part = part_allocate(self->arg);
	self->part = part;

	// create indexes and stores
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);
		part_index_create(part, config);
	}

	// create flat stores
	list_foreach(&self->arg->columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->size_flat)
			continue;

		auto flat = flat_allocate(column, 0);
		flats_add_column(&part->flats);
		flats_add(&part->flats, flat);
	}

	// create pods and load heaps
	self->iface->attach(part);
}

void
parts_close(Parts* self)
{
	if (! self->part)
		return;

	// drop pods
	self->iface->detach(self->part);
}

void
parts_truncate(Parts* self)
{
	self->iface->truncate(self->part);
}

void
parts_index_create(Parts* self, IndexConfig* config)
{
	part_index_create(self->part, config);
}

void
parts_index_remove(Parts* self, Str* name)
{
	part_index_drop(self->part, name);
}

void
parts_column_create(Parts* self, Column* column, uint32_t timeline)
{
	if (column->type != TYPE_VECTOR)
		return;

	// create flat stores
	auto part = self->part;
	auto flat = flat_allocate(column, timeline);
	flats_add_column(&part->flats);
	flats_add(&part->flats, flat);
}
