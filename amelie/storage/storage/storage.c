
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

#include <amelie_runtime>
#include <amelie_tier>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_init(Storage*   self,
             CatalogIf* iface,
             void*      iface_arg,
             DeployIf*  iface_deploy,
             void*      iface_deploy_arg)
{
	lock_mgr_init(&self->lock_mgr);
	catalog_init(&self->catalog, iface, iface_arg, &self->deploy);
	service_init(&self->service);
	service_mgr_init(&self->service_mgr);
	deploy_init(&self->deploy, iface_deploy, iface_deploy_arg);
	wal_mgr_init(&self->wal_mgr);
	compaction_mgr_init(&self->compaction_mgr);
}

void
storage_free(Storage* self)
{
	catalog_free(&self->catalog);
	service_free(&self->service);
	deploy_free(&self->deploy);
	wal_mgr_free(&self->wal_mgr);
	lock_mgr_free(&self->lock_mgr);
}

void
storage_open(Storage* self)
{
	// prepare deploy hash
	deploy_prepare(&self->deploy);

	// prepare system catalog
	catalog_open(&self->catalog);
}

void
storage_close(Storage* self)
{
	// shutdown service and compaction
	service_shutdown(&self->service);
	compaction_mgr_stop(&self->compaction_mgr);

	// close catalog
	catalog_close(&self->catalog);

	// stop wal mgr
	wal_mgr_stop(&self->wal_mgr);
}

Buf*
storage_state(Storage* self)
{
	unused(self);

	// {}
	auto buf = buf_create();
	encode_obj(buf);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	// directory
	encode_raw(buf, "directory", 9);
	encode_string(buf, &state()->directory.string);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_string(buf, &config()->uuid.string);

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_integer(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_integer(buf, opt_int_of(&config()->backends));

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, state_checkpoint());

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, state_lsn());

	// read_only
	encode_raw(buf, "read_only", 9);
	encode_bool(buf, opt_int_of(&state()->read_only));

	encode_obj_end(buf);
	return buf;
}

void
storage_lock(Storage* self, ServiceLock* lock, Id* id)
{
	// get shared catalog lock and hold till unlock
	lock_catalog(&self->lock_mgr, LOCK_SHARED);

	// get service lock for partition id
	service_mgr_lock(&self->service_mgr, lock, id);
}

void
storage_unlock(Storage* self, ServiceLock* lock)
{
	// unlock partition
	service_mgr_unlock(&self->service_mgr, lock);

	// unlock catalog
	unlock_catalog(&self->lock_mgr, LOCK_SHARED);
}
