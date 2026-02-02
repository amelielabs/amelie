
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_object.h>
#include <amelie_engine.h>

void
engine_set_unlogged(Engine* self, bool value)
{
	self->unlogged = value;
	list_foreach(&self->levels->list)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
engine_truncate(Engine* self)
{
	list_foreach(&self->levels->list)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
engine_index_add(Engine* self, IndexConfig* config)
{
	list_foreach(&self->levels->list)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
engine_index_remove(Engine* self, Str* name)
{
	list_foreach(&self->levels->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}
