
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
#include <amelie_lock.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

static void
storage_execute(Storage* self, Refresh* refresh, ServiceReq* req)
{
	while (req->current < req->actions_count)
	{
		auto action = &req->actions[req->current];
		switch (action->type) {
		case ACTION_REFRESH:
			refresh_reset(refresh);
			refresh_run(refresh, &req->id, NULL, true);
			break;
		case ACTION_WAL_GC:
			// todo:
			(void)self;
			break;
		default:
			break;
		}
		req->current++;
	}
}

bool
storage_service(Storage* self, Refresh* refresh, bool wait)
{
	ServiceReq* req = NULL;
	bool shutdown = service_next(&self->service, wait, &req);
	if (shutdown)
		return true;
	if (req == NULL)
		return false;
	defer(service_req_free, req);
	error_catch (
		storage_execute(self, refresh, req)
	);
	return false;
}
