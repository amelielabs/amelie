
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
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

void
meta_mgr_init(MetaMgr* self, MetaIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	handle_mgr_init(&self->mgr);
}

void
meta_mgr_free(MetaMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
meta_mgr_create(MetaMgr*     self,
                Transaction* trx,
                MetaConfig*  config,
                bool         if_not_exists)
{
	// make sure meta does not exists
	auto current = meta_mgr_find(self, &config->schema, &config->name, false);
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
	handle_mgr_write(&self->mgr, trx, LOG_CREATE_META, &meta->handle, op);

	buf_unpin(op);
	unguard(&guard);
}

void
meta_mgr_drop(MetaMgr*     self,
              Transaction* trx,
              Str*         schema,
              Str*         name,
              bool         if_exists)
{
	auto meta = meta_mgr_find(self, schema, name, false);
	if (! meta)
	{
		if (! if_exists)
			error("object '%.*s': not exists", str_size(name),
			      str_of(name));
		return;
	}

	// save drop table operation
	auto op = meta_op_drop(schema, name);

	// drop by name
	Handle drop;
	handle_init(&drop);
	handle_set_schema(&drop, schema);
	handle_set_name(&drop, name);

	// update mgr
	handle_mgr_write(&self->mgr, trx, LOG_DROP_META, &drop, op);

	buf_unpin(op);
}

void
meta_mgr_dump(MetaMgr* self, Buf* buf)
{
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto meta = meta_of(list_at(Handle, link));
		meta_config_write(meta->config, buf);
	}
}

Meta*
meta_mgr_find(MetaMgr* self, Str* schema, Str* name,
              bool error_if_not_exists)
{
	auto handle = handle_mgr_get(&self->mgr, schema, name);
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
meta_mgr_list(MetaMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);
	// map
	encode_map(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// name: {}
		auto meta = meta_of(list_at(Handle, link));
		encode_string(buf, &meta->config->name);
		meta_config_write(meta->config, buf);
	}
	msg_end(buf);
	return buf;
}
