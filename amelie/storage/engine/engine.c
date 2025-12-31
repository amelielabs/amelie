
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
#include <amelie_volume>
#include <amelie_wal.h>
#include <amelie_engine.h>

void
engine_init(Engine* self, WalMgr* wal_mgr, World* world)
{
	self->world   = world;
	self->wal_mgr = wal_mgr;
	service_init(&self->service);
}

void
engine_free(Engine* self)
{
	service_free(&self->service);
}
