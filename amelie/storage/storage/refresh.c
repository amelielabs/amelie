
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
refresh_init(Refresh* self, Storage* storage)
{
	self->origin        = NULL;
	self->part          = NULL;
	self->heap          = NULL;
	self->volume_origin = NULL;
	self->volume        = NULL;
	self->storage       = storage;
	merger_init(&self->merger);
}

void
refresh_free(Refresh* self)
{
	merger_free(&self->merger);
}

void
refresh_reset(Refresh* self)
{
	self->origin        = NULL;
	self->part          = NULL;
	self->heap          = NULL;
	self->volume_origin = NULL;
	self->volume        = NULL;
	merger_reset(&self->merger);
}

void
refresh_run(Refresh* self, Id* id, Str* tier, bool if_exists)
{
	(void)self;
	(void)id;
	(void)tier;
	(void)if_exists;
}
