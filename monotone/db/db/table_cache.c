
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
#include <monotone_db.h>

static void
table_if_free(Handle* self, void* arg)
{
	unused(arg);
	auto table = table_of(self);
	table_free(table);
}

static Handle*
table_if_copy(Handle* self, void* arg)
{
	unused(arg);
	auto table = table_of(self);
	auto copy = table_allocate(table->config);
	copy->handle.lsn = table->handle.lsn;
	return &copy->handle;
}

HandleIf table_if =
{
	.free = table_if_free,
	.copy = table_if_copy
};

void
table_cache_init(TableCache* self)
{
	handle_cache_init(&self->cache);
}

void
table_cache_free(TableCache* self)
{
	handle_cache_free(&self->cache);
}

void
table_cache_sync(TableCache* self, TableCache* with)
{
	handle_cache_sync(&self->cache, &with->cache);
}

void
table_cache_dump(TableCache* self, Buf* buf)
{
	// array
	encode_array(buf, self->cache.list_count);
	list_foreach(&self->cache.list)
	{
		auto table = table_of(list_at(Handle, link));
		table_config_write(table->config, buf);
	}
}

Table*
table_cache_find(TableCache* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_cache_get(&self->cache, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("table '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return table_of(handle);
}

Table*
table_cache_find_by_id(TableCache* self, Uuid* id)
{
	list_foreach(&self->cache.list)
	{
		auto table = table_of(list_at(Handle, link));
		if (uuid_compare(&table->config->id, id))
			return table;
	}
	return NULL;
}

Buf*
table_cache_list(TableCache* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// map
	encode_map(buf, self->cache.list_count);
	list_foreach(&self->cache.list)
	{
		// name: {}
		auto table = table_of(list_at(Handle, link));
		encode_string(buf, &table->config->name);
		table_config_write(table->config, buf);
	}
	msg_end(buf);
	return buf;
}
