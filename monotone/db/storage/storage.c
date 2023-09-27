
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
#include <monotone_storage.h>

Storage*
storage_allocate(StorageConfig* config)
{
	Storage* self = mn_malloc(sizeof(Storage));
	self->refs          = 0;
	self->config        = config;
	self->indexes_count = 0;
	list_init(&self->indexes);
	list_init(&self->link_list);
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

hot void
storage_write(Storage*     self,
              Transaction* trx,
              LogCmd       cmd,
              bool         unique,
              uint8_t*     data,
              int          data_size)
{

	auto primary = storage_primary(self);

	// allocate row
	auto row = row_create(&primary->config->schema, data, data_size);
	guard(row_guard, row_free, row);

	// update primary index
	if (cmd == LOG_REPLACE)
	{
		auto prev = index_set(primary, trx, row);
		if (unlikely(unique && prev))
			error("unique key constraint violation");
		unguard(&row_guard);
	} else {
		index_delete(primary, trx, row);
		// delete row
	}
}

Index*
storage_find(Storage* self, Str* index_name)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		if (str_compare(&index->config->name, index_name))
			return index;
	}
	return NULL;
}
