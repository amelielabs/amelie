
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

static void
service_schedule(Service* self, Action* action)
{
	// schedule service operation

	// find table
	auto table = table_mgr_find_by(&self->catalog->table_mgr, &action->req->id_table, false);
	if (!table || !tier_mgr_created(&table->tier_mgr))
		return;

	// take shared table lock
	auto lock_table = lock(&table->rel, LOCK_SHARED);
	// todo:

	unlock(lock_table);
}

void
service_execute(Service* self, Action* action)
{
	// take shared catalog lock
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_SHARED);
	defer(unlock, lock_catalog);

	// find table and schedule next service action
	service_schedule(self, action);

	// execute
	switch (action->type) {
	case ACTION_FLUSH:
		service_flush(self, &action->req->id_table, action->id);
		break;
	case ACTION_REFRESH:
		service_refresh_object(self, &action->req->id_table, action->id, NULL);
		break;
	case ACTION_NONE:
		break;
	}
}
