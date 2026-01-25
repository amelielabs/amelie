
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
#include <amelie_tier.h>
#include <amelie_partition.h>

static void
closedir_defer(DIR* self)
{
	closedir(self);
}

static void
part_mgr_recover_storage(PartMgr* self, Tier* tier, TierStorage* storage)
{
	char id[UUID_SZ];
	uuid_get(&tier->config->id, id, sizeof(id));

	// <storage_path>/<tier>
	char path[PATH_MAX];
	storage_pathfmt(storage->storage, path, "%s", id);

	// read directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("tier: directory '%s' open error", path);
	defer(closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;

		// <id>.heap
		// <id>.heap.incomplete
		int64_t psn;
		auto state = id_of(entry->d_name, &psn);
		switch (state) {
		case ID_HEAP:
		case ID_HEAP_INCOMPLETE:
			break;
		default:
			continue;
		}
		state_psn_follow(psn);

		// find existing partition by id
		auto part = part_mgr_find(self, psn);
		if (part)
		{
			// expect to share the same storage
			assert(part->id.storage == storage->storage);
			id_set(&part->id, state);
			continue;
		}

		// add partition
		Id id =
		{
			.id       = psn,
			.id_tier  = tier->config->id,
			.id_table = self->tier_mgr->id_table,
			.state    = state,
			.storage  = storage->storage,
			.tier     = tier,
			.encoding = &tier->encoding
		};
		part = part_allocate(self->tier_mgr, &id, self->seq, self->unlogged);
		part_mgr_add(self, part);
	}
}

void
part_mgr_recover(PartMgr* self, List* indexes)
{
	auto main = tier_mgr_main(self->tier_mgr);
	list_foreach(&main->config->storages)
	{
		auto storage = list_at(TierStorage, link);
		part_mgr_recover_storage(self, main, storage);
	}

	// validate and open partitions
	list_foreach_safe(&self->parts)
	{
		auto part = list_at(Part, link);

		// crash recovery
		auto id = &part->id;
		switch (id->state) {
		case ID_HEAP|ID_HEAP_INCOMPLETE:
		{
			// crash before completion
			id_delete(&part->id, ID_HEAP_INCOMPLETE);
			id_unset(id, ID_HEAP_INCOMPLETE);
			break;
		}
		case ID_HEAP_INCOMPLETE:
		{
			// crash after sync/unlink
			id_rename(&part->id, ID_HEAP_INCOMPLETE, ID_HEAP);
			id_unset(id, ID_HEAP_INCOMPLETE);
			id_set(id, ID_HEAP);
			break;
		}
		case ID_HEAP:
		{
			// correct state
			break;
		}
		default:
			error("tier: invlid partition state");
			break;
		}

		// add indexes
		list_foreach(indexes)
		{
			auto config = list_at(IndexConfig, link);
			part_index_add(part, config);
		}

		// load partition heap
		part_open(part);

		// add partition mapping
		part_mapping_add(&self->mapping, part);

		// set object metrics
		track_set_lsn(&part->track, part->heap->header->lsn);
	}
}

void
part_mgr_deploy(PartMgr* self, int count)
{
	// create initial hash partitions
	if (count < 1 || count > PART_MAPPING_MAX)
		error("table has invalid partitions number");

	// prepare empty heap
	auto heap = heap_allocate();
	defer(heap_free, heap);

	auto main = tier_mgr_main(self->tier_mgr);
	auto main_storage = tier_storage(main);

	// hash partition_max / hash partitions
	int range_max      = PART_MAPPING_MAX;
	int range_interval = range_max / count;
	int range_start    = 0;
	for (auto order = 0; order < count; order++)
	{
		// set hash range
		int range_step;
		auto is_last = (order == count - 1);
		if (is_last)
			range_step = range_max - range_start;
		else
			range_step = range_interval;
		if ((range_start + range_step) > range_max)
			range_step = range_max - range_start;

		auto hash_min = range_start;
		auto hash_max = range_start + range_step;
		range_start += range_step;

		// set heap hash range
		heap->header->hash_min = hash_min;
		heap->header->hash_max = hash_max;

		// create <id>.heap.incomplete file
		File file;
		file_init(&file);
		defer(file_close, &file);

		Id id =
		{
			.id       = state_psn_next(),
			.id_tier  = main->config->id,
			.id_table = self->tier_mgr->id_table,
			.state    = 0,
			.storage  = main_storage,
			.tier     = main,
			.encoding = &main->encoding
		};
		heap_create(heap, &file, &id, ID_HEAP_INCOMPLETE);

		// sync
		if (main_storage->config->sync)
			file_sync(&file);

		// rename
		id_rename(&id, ID_HEAP_INCOMPLETE, ID_HEAP);
	}
}
