
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
#include <monotone_wal.h>
#include <monotone_db.h>

void
meta_mgr_init(MetaMgr* self, MetaIface* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	handle_mgr_init(&self->metas);
	handle_mgr_set_free(&self->metas, meta_destructor, NULL);
}

void
meta_mgr_free(MetaMgr* self)
{
	handle_mgr_free(&self->metas);
}

void
meta_mgr_gc(MetaMgr* self, uint64_t snapshot_min)
{
	handle_mgr_gc(&self->metas, snapshot_min);
}

void
meta_mgr_create(MetaMgr*     self,
                Transaction* trx,
                MetaConfig*  config,
                bool         if_not_exists)
{
	// make sure meta does not exists
	auto current = handle_mgr_get(&self->metas, &config->name);
	if (current)
	{
		if (! if_not_exists)
			error("object '%.*s': already exists", str_size(&config->name),
			      str_of(&config->name));
		return;
	}

	// allocate meta and init
	auto meta = meta_allocate(config, self->iface, self->iface_arg);
	guard(guard, meta_free, meta);

	// save create meta operation
	auto op = meta_op_create(config);

	// update metas
	mvcc_write_handle(trx, LOG_CREATE_META, &self->metas, &meta->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

void
meta_mgr_drop(MetaMgr*     self,
              Transaction* trx,
              Str*         name,
              bool         if_exists)
{
	auto current = handle_mgr_get(&self->metas, name);
	if (! current)
	{
		if (! if_exists)
			error("object '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}
	auto meta = meta_of(current);

	// create new drop object
	auto update = meta_allocate(meta->config, NULL, NULL);
	update->handle.drop = true;
	guard(guard, meta_free, update);

	// save drop table operation
	auto op = meta_op_drop(&meta->config->name);

	// update metas
	mvcc_write_handle(trx, LOG_DROP_META, &self->metas, &update->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

Meta*
meta_mgr_find(MetaMgr* self, Str* name, bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->metas, name);
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
meta_mgr_show(MetaMgr* self, MetaType type)
{
	auto buf = msg_create(MSG_OBJECT);

	// map
	encode_map32(buf, 0);

	int count = 0;
	list_foreach(&self->metas.list)
	{
		auto head = list_at(Handle, link);
		if (head->drop)
			continue;
		auto meta = meta_of(head);
		if (meta->config->type != type)
			continue;

		// "name": {}
		encode_string(buf, &meta->config->name);
		meta_config_write(meta->config, buf);
		count++;
	}

	// update count
	uint8_t* map_pos = buf->start + sizeof(Msg);
	data_write_map32(&map_pos, count);

	msg_end(buf);
	return buf;
}
