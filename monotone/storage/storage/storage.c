
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>

Storage*
storage_create(StorageConfig* config, CompactMgr* compact_mgr)
{
	Storage* self = mn_malloc(sizeof(Storage));
	self->refs   = 0;
	self->config = config;
	engine_init(&self->engine, compact_mgr,
	            &config->id,
	            &config->schema,
	             config->compression,
	             config->crc);
	return self;
}

void
storage_ref(Storage* self)
{
	self->refs++;
}

void
storage_unref(Storage* self)
{
	self->refs--;
	if (self->refs >= 0)
		return;
	engine_stop(&self->engine);
	engine_free(&self->engine);
	mn_free(self);
}

void
storage_open(Storage* self, bool start_service)
{
	engine_start(&self->engine, start_service);
}

hot void
storage_write(Storage*     self,
              Transaction* trx,
              LogCmd       cmd,
              bool         unique,
              uint8_t*     data,
              int          data_size)
{
	// allocate row
	auto row = row_create(&self->config->schema, data, data_size);
	if (cmd == LOG_DELETE)
		row->is_delete = true;
	guard(row_guard, row_free, row);

	// get partition and obtain shared lock
	auto ref  = part_map_get(&self->engine.map, row);
	auto lock = lock_lock(&ref->lock, true);
	auto part = ref->part;
	guard(lock_guard, lock_unlock, lock);

	// update heap
	bool update;
	update = mvcc_write(trx, cmd, &part->heap, lock, row);
	if (unlikely(unique && update))
		error("unique key constraint violation");

	unguard(&row_guard);
	unguard(&lock_guard);

	// schedule partition service
	if (heap_is_full(&part->heap))
		service_add(&self->engine.service, part);
}

Iterator*
storage_read(Storage* self, Str* index_name)
{
	unused(index_name);
	return storage_iterator_allocate(self);
}
