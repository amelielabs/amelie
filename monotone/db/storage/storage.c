
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>

Storage*
storage_allocate(StorageConfig* config)
{
	Storage* self = mn_malloc(sizeof(Storage));
	self->refs          = 0;
	self->config        = config;
	self->indexes_count = 0;
	list_init(&self->indexes);
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
	if (self->config)
		storage_config_free(self->config);
	mn_free(self);
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
}

void
storage_attach(Storage* self, Index* index)
{
	list_append(&self->indexes, &index->link);
	self->indexes_count++;
}

void
storage_detach(Storage* self, Index* index)
{
	list_unlink(&index->link);
	self->indexes_count--;
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

hot void
storage_set(Storage*     self,
            Transaction* trx,
            bool         unique,
            uint8_t*     data,
            int          data_size)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update primary index
	auto prev = index_set(primary, trx, row);
	unguard(&row_guard);

	if (unlikely(unique && prev))
		error("unique key constraint violation");
}

hot void
storage_update(Storage*     self,
               Transaction* trx,
               Iterator*    it,
               uint8_t*     data,
               int          data_size)
{
	auto primary = storage_primary(self);

	// todo: primary iterator only

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update primary index
	index_update(primary, trx, it, row);
	unguard(&row_guard);
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
                  uint8_t*     data,
                  int          data_size)
{
	auto primary = storage_primary(self);

	// allocate row
	auto key = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, key);

	// delete from primary index by key
	index_delete_by(primary, trx, key);
}

hot bool
storage_upsert(Storage*     self,
               Transaction* trx,
               Iterator*    it,
               uint8_t*     data,
               int          data_size)
{
	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->def, data, data_size);
	guard(row_guard, row_free, row);

	// update index
	bool updated = index_upsert(primary, trx, it, row);
	if (updated)
		unguard(&row_guard);
	return updated;
}
