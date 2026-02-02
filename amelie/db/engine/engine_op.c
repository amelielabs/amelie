
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
	list_foreach(&engine_main(self)->list)
	{
		auto part = list_at(Part, link);
		part->unlogged = value;
	}
}

void
engine_truncate(Engine* self)
{
	list_foreach(&engine_main(self)->list)
	{
		auto part = list_at(Part, link);
		part_truncate(part);
	}
}

void
engine_index_add(Engine* self, IndexConfig* config)
{
	list_foreach(&engine_main(self)->list)
	{
		auto part = list_at(Part, link);
		part_index_add(part, config);
	}
}

void
engine_index_remove(Engine* self, Str* name)
{
	list_foreach(&engine_main(self)->list)
	{
		auto part = list_at(Part, link);
		part_index_drop(part, name);
	}
}

Level*
engine_tier_find(Engine* self, Str* name)
{
	list_foreach(&self->levels)
	{
		auto level = list_at(Level, link);
		if (str_compare(&level->tier->name, name))
			return level;
	}
	return NULL;
}

void
engine_tier_add(Engine* self, Tier* tier)
{
	auto level = level_allocate(tier, self->mapping.keys);
	list_append(&self->levels, &level->link);
	self->levels_count++;
}

void
engine_tier_remove(Engine* self, Str* name)
{
	auto level = engine_tier_find(self, name);
	assert(level);
	list_unlink(&level->link);
	self->levels_count--;
}
