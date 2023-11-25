
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
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_semantic.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_hub.h>
#include <monotone_session.h>

void
session_lock(Session* self, SessionLock lock)
{
	assert(self->lock == LOCK_NONE);

	switch (lock) {
	case LOCK_SHARED:
		self->lock_shared = lock_lock(self->share->cat_lock, true);
		self->lock        = lock;
		break;
	case LOCK_EXCLUSIVE:
	{
		self->lock = lock;

		auto req = &self->lock_req;
		req->on_lock = condition_create();

		// RPC_CAT_LOCK_REQ
		channel_write(global()->control->core, cat_lock_msg(req));

		// wait for exclusive lock to be taken on all hubs (including this one)
		condition_wait(req->on_lock, -1);
		break;
	}
	case LOCK_NONE:
		break;
	}
}

void
session_unlock(Session* self)
{
	switch (self->lock) {
	case LOCK_SHARED:
	{
		lock_unlock(self->lock_shared);
		self->lock_shared = NULL;
		break;
	}
	case LOCK_EXCLUSIVE:
	{
		// ask core to unlock hubs
		auto req = &self->lock_req;
		if (req->on_lock)
		{
			condition_signal(req->on_unlock);
			condition_free(req->on_lock);
		}
		break;
	}
	case LOCK_NONE:
		break;
	}
	self->lock = LOCK_NONE;
}
