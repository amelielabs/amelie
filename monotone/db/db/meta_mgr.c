
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

void
meta_mgr_init(MetaMgr* self, MetaIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	meta_cache_init(&self->cache);
}

void
meta_mgr_free(MetaMgr* self)
{
	meta_cache_free(&self->cache);
}

void
meta_mgr_create(MetaMgr*     self,
                Transaction* trx,
                MetaConfig*  config,
                bool         if_not_exists)
{
	// make sure meta does not exists
	auto current = meta_cache_find(&self->cache, &config->name, false);
	if (current)
	{
		if (! if_not_exists)
			error("object '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate meta and init
	auto meta = meta_allocate(config, self->iface);
	guard(guard, meta_free, meta);

	// prepare meta data
	meta_data_init(meta, self->iface_arg);

	// save create meta operation
	auto op = meta_op_create(config);

	// update metas
	mvcc_write_handle(trx, LOG_CREATE_META, &self->cache.cache, &meta->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

void
meta_mgr_drop(MetaMgr*     self,
              Transaction* trx,
              Str*         name,
              bool         if_exists)
{
	auto meta = meta_cache_find(&self->cache, name, false);
	if (! meta)
	{
		if (! if_exists)
			error("object '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// save drop table operation
	auto op = meta_op_drop(name);

	// drop by name
	Handle drop;
	handle_init(&drop);
	drop.name = name;

	// update cache
	mvcc_write_handle(trx, LOG_DROP_META, &self->cache.cache, &drop, op);

	buf_unpin(op);
}
