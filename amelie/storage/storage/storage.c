
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
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
storage_init(Storage*   self,
             CatalogIf* iface,
             void*      iface_arg,
             PartAttach attach,
             void*      attach_arg)
{
	part_mgr_init(&self->part_mgr, attach, attach_arg);
	catalog_init(&self->catalog, &self->part_mgr, iface, iface_arg);
	checkpoint_mgr_init(&self->checkpoint_mgr, &storage_checkpoint_if, self);
	checkpointer_init(&self->checkpointer, &self->checkpoint_mgr);
	wal_mgr_init(&self->wal_mgr);
}

void
storage_free(Storage* self)
{
	catalog_free(&self->catalog);
	part_mgr_free(&self->part_mgr);
	checkpoint_mgr_free(&self->checkpoint_mgr);
	wal_mgr_free(&self->wal_mgr);
}

void
storage_open(Storage* self)
{
	// prepare system catalog
	catalog_open(&self->catalog);

	// read directory and restore last checkpoint catalog
	// (databases, tables)
	checkpoint_mgr_open(&self->checkpoint_mgr);
}

void
storage_close(Storage* self)
{
	// stop checkpointer service
	checkpointer_stop(&self->checkpointer);

	// close catalog
	catalog_close(&self->catalog);

	// free partition mgr
	part_mgr_free(&self->part_mgr);

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

	// tsn
	encode_raw(buf, "tsn", 3);
	encode_integer(buf, state_tsn());

	// psn
	encode_raw(buf, "psn", 3);
	encode_integer(buf, state_psn());

	// read_only
	encode_raw(buf, "read_only", 9);
	encode_bool(buf, opt_int_of(&state()->read_only));

	encode_obj_end(buf);
	return buf;
}
