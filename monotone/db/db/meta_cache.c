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
meta_if_free(Handle* self, void* arg)
{
	unused(arg);
	auto meta = meta_of(self);
	meta_free(meta);
}

static Handle*
meta_if_copy(Handle* self, void* arg)
{
	unused(arg);
	auto meta = meta_of(self);
	auto copy = meta_allocate(meta->config, meta->iface);
	copy->handle.lsn = meta->handle.lsn;
	guard(guard, meta_free, copy);
	meta_data_copy(copy, meta);

	unguard(&guard);
	return &copy->handle;
}

HandleIf meta_if =
{
	.free = meta_if_free,
	.copy = meta_if_copy
};

void
meta_cache_init(MetaCache* self)
{
	handle_cache_init(&self->cache);
}

void
meta_cache_free(MetaCache* self)
{
	handle_cache_free(&self->cache);
}

void
meta_cache_sync(MetaCache* self, MetaCache* with)
{
	handle_cache_sync(&self->cache, &with->cache);
}

void
meta_cache_dump(MetaCache* self, Buf* buf)
{
	// array
	encode_array(buf, self->cache.list_count);
	list_foreach(&self->cache.list)
	{
		auto meta = meta_of(list_at(Handle, link));
		meta_config_write(meta->config, buf);
	}
}

Meta*
meta_cache_find(MetaCache* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_cache_get(&self->cache, name);
	if (! handle)
	{
		if (error_if_not_exists)
			error("object '%.*s': not exists", str_size(name),
			      str_of(name));
		return NULL;
	}
	return meta_of(handle);
}

Buf*
meta_cache_list(MetaCache* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// map
	encode_map(buf, self->cache.list_count);
	list_foreach(&self->cache.list)
	{
		// name: {}
		auto meta = meta_of(list_at(Handle, link));
		encode_string(buf, &meta->config->name);
		meta_config_write(meta->config, buf);
	}
	msg_end(buf);
	return buf;
}
