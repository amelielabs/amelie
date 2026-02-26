
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

void
service_init(Service* self, Catalog* catalog, WalMgr* wal_mgr)
{
	self->catalog = catalog;
	self->wal_mgr = wal_mgr;
	ops_init(&self->ops);
}

void
service_free(Service* self)
{
	ops_free(&self->ops);
}
