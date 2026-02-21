
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
engine_init(Engine*     self,
            EngineIf*   iface,
            void*       iface_arg,
            StorageMgr* storage_mgr,
            Uuid*       id_table,
            Sequence*   seq,
            bool        unlogged,
            Keys*       keys)
{
	self->iface        = iface;
	self->iface_arg    = iface_arg;
	self->storage_mgr  = storage_mgr;
	self->levels_count = 0;
	self->id_table     = id_table;
	self->seq          = seq;
	self->unlogged     = unlogged;
	list_init(&self->levels);
	part_mapping_init(&self->mapping, keys);
}

void
engine_free(Engine* self)
{
	list_foreach_safe(&self->levels)
	{
		auto level = list_at(Level, link);
		level_free(level);
	}
	part_mapping_free(&self->mapping);
}

void
engine_open(Engine* self, List* tiers, List* indexes, int count)
{
	// create levels
	list_foreach(tiers)
	{
		auto tier = list_at(Tier, link);
		engine_tier_add(self, tier);
	}

	// recover files
	engine_recover(self, count);

	// create indexes
	auto main = engine_main(self);
	list_foreach(&main->list)
	{
		auto part = list_at(Part, link);
		list_foreach(indexes)
		{
			auto config = list_at(IndexConfig, link);
			part_index_create(part, config);
		}
	}

	// create pods and load heaps
	self->iface->attach(self, main);

	// map hash partitions
	list_foreach(&main->list)
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
engine_close(Engine* self, bool drop)
{
	if (! self->levels_count)
		return;

	// drop pods
	auto main = engine_main(self);
	self->iface->detach(self, main);

	// delete partition files on drop
	if (drop)
	{
		list_foreach(&main->list)
		{
			auto part = list_at(Part, link);
			id_delete(&part->id, ID_RAM);
		}
	}
}

Part*
engine_find(Engine* self, uint64_t id)
{
	list_foreach(&engine_main(self)->list)
	{
		auto part = list_at(Part, link);
		if (part->id.id == id)
			return part;
	}
	return NULL;
}

Buf*
engine_status(Engine* self, Str* ref, bool extended)
{
	auto buf = buf_create();
	errdefer_buf(buf);

	// show partition id on table
	if (ref)
	{
		int64_t id;
		if (str_toint(ref, &id) == -1)
			error("invalid partition id");

		auto part = engine_find(self, id);
		if (! part)
			encode_null(buf);
		else
			part_status(part, buf, extended);
		return buf;
	}

	// show partitions on table
	encode_array(buf);
	auto main = engine_main(self);
	list_foreach(&main->list)
	{
		auto part = list_at(Part, link);
		part_status(part, buf, extended);
	}

	list_foreach(&main->list_pending)
	{
		auto obj = list_at(Object, link);
		object_status(obj, ID_PENDING, buf, extended);
	}
	encode_array_end(buf);
	return buf;
}
