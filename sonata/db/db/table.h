#pragma once

//
// sonata.
//
// SQL Database for JSON.
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
	so_free(self);
}

static inline Table*
table_allocate(TableConfig* config)
{
	Table* self = so_malloc(sizeof(Table));
	self->config = NULL;
	storage_mgr_init(&self->storage_mgr);
	serial_init(&self->serial);
	guard(table_free, self);
	self->config = table_config_copy(config);

	handle_init(&self->handle);
	handle_set_schema(&self->handle, &self->config->schema);
	handle_set_name(&self->handle, &self->config->name);
	handle_set_free_function(&self->handle, (HandleFree)table_free);
	return unguard();
}

static inline void
table_open(Table* self)
{
	storage_mgr_open(&self->storage_mgr, self->config->reference,
	                 &self->config->storages,
	                 &self->config->indexes);
}

static inline void
table_recover(Table* self, Uuid* shard)
{
	storage_mgr_recover(&self->storage_mgr, shard);
}

static inline Table*
table_of(Handle* handle)
{
	return (Table*)handle;
}

hot static inline IndexConfig*
table_find_index(Table* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->config->indexes)
	{
		auto config = list_at(IndexConfig, link);
		if (str_compare(&config->name, name))
			return config;
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
