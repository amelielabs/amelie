#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Table Table;

struct Table
{
	Handle       handle;
	StorageMgr   storage_mgr;
	Serial       serial;
	TableConfig* config;
};

static inline void
table_free(Table* self)
{
	storage_mgr_free(&self->storage_mgr);
	if (self->config)
		table_config_free(self->config);
	mn_free(self);
}

static inline Table*
table_allocate(TableConfig* config)
{
	Table* self = mn_malloc(sizeof(Table));
	self->config = NULL;
	storage_mgr_init(&self->storage_mgr);
	serial_init(&self->serial);
	guard(self_guard, table_free, self);
	self->config = table_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)table_free);
	return unguard(&self_guard);
}

static inline void
table_open(Table* self)
{
	storage_mgr_open(&self->storage_mgr, self->config->map,
	                 &self->config->id,
	                 &self->config->storages,
	                 &self->config->indexes);
}

static inline Table*
table_of(Handle* handle)
{
	return (Table*)handle;
}

hot static inline Def*
table_find_index_def(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare(&config->name, name))
			return &config->def;
	}

	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));

	return NULL;
}

static inline Def*
table_def(Table* self)
{
	auto primary = container_of(self->config->indexes.next, IndexConfig, link);
	return &primary->def;
}

hot static inline Part*
table_map(Table* self, Uuid* shard, uint8_t* data, int data_size)
{
	// find storage related to the shard
	auto storage = storage_mgr_find(&self->storage_mgr, shard);
	assert(storage);

	// find or create partition
	bool created = false;
	auto part = storage_map(storage, data, data_size, &created);
	if (created)
		part_open(part, &self->config->indexes);

	return part;
}
