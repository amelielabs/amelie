
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>

Storage*
storage_allocate(StorageConfig* config, Uuid* table)
{
	auto self = (Storage*)so_malloc(sizeof(Storage));
	self->table         = table;
	self->config        = config;
	self->indexes_count = 0;
	snapshot_mgr_init(&self->snapshot_mgr);
	list_init(&self->indexes);
	list_init(&self->link_cp);
	list_init(&self->link);
	return self;
}

void
storage_free(Storage* self)
{
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	snapshot_mgr_free(&self->snapshot_mgr);
	so_free(self);
}

void
storage_open(Storage* self, List* indexes)
{
	list_foreach(indexes)
	{
		auto config = list_at(IndexConfig, link);

		// create tree index
		auto index = tree_allocate(config, self->table, &self->config->id);
		list_append(&self->indexes, &index->link);
		self->indexes_count++;
	}

	// read or create storage repository
	snapshot_mgr_open(&self->snapshot_mgr, &self->config->id);
}

void
storage_recover(Storage* self)
{
	uint64_t snapshot = snapshot_mgr_last(&self->snapshot_mgr);
	if (snapshot == UINT64_MAX)
		return;

	auto primary = storage_primary(self);

	SnapshotId id;
	snapshot_id_set(&id, &self->config->id, snapshot);

	log("recover %" PRIu64 ": %s begin", snapshot, id.uuid_sz);

	SnapshotCursor cursor;
	snapshot_cursor_init(&cursor);
	guard(snapshot_cursor_close, &cursor);

	snapshot_cursor_open(&cursor, &id);
	for (;;)
	{
		auto buf = snapshot_cursor_next(&cursor);
		if (! buf)
			break;
		guard_buf(buf);

		// allocate row
		auto pos = msg_of(buf)->data;
		auto row = row_create(&primary->config->def, &pos);
		guard(row_free, row);

		// update primary index
		index_ingest(primary, row);
		unguard();

		// todo: secondary indexes
	}

	// set index lsn
	primary->lsn = snapshot;

	log("recover %" PRIu64 ": %s complete", snapshot, id.uuid_sz);
}

hot void
storage_set(Storage*     self,
            Transaction* trx,
            bool         unique,
            uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// update primary index
	auto prev = index_set(primary, trx, row);
	unguard();

	if (unlikely(unique && prev))
		error("unique key constraint violation");
}

hot void
storage_update(Storage*     self,
               Transaction* trx,
               Iterator*    it,
               uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// todo: primary iterator only

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// update primary index
	index_update(primary, trx, it, row);
	unguard();
}

hot void
storage_delete(Storage*     self,
               Transaction* trx,
               Iterator*    it)
{
	// todo: primary iterator only

	// update primary index
	auto primary = storage_primary(self);
	index_delete(primary, trx, it);
}

void
storage_delete_by(Storage*     self,
                  Transaction* trx,
                  uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto key = row_create(&primary->config->def, pos);
	guard(row_free, key);

	// delete from primary index by key
	index_delete_by(primary, trx, key);
}

hot void
storage_upsert(Storage*     self,
               Transaction* trx,
               Iterator**   it,
               uint8_t**    pos)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, pos);
	guard(row_free, row);

	// do insert or return iterator
	index_upsert(primary, trx, it, row);
	if (*it == NULL)
		unguard();
}

Index*
storage_find(Storage* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		if (str_compare(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
