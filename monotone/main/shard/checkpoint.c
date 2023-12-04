
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
#include <monotone_index.h>
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

static void
checkpoint_create(Checkpoint* self)
{
	list_foreach(&self->db->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		auto storage = storage_mgr_find_by_shard(&table->storage_mgr, self->shard);
		if (storage == NULL)
			continue;

		for (;;)
		{
			// take exclusive lock
			auto lock = checkpoint_lock(self, false);
			auto part = storage_schedule(storage, self->lsn);
			checkpoint_unlock(lock);
			if (! part)
				break;

			part_snapshot(part, &self->db->snapshot_writer, &self->snapshot);
		}
	}

	self->active = false;

	snapshot_free(&self->snapshot);
	snapshot_init(&self->snapshot);

	// notify caller
	if (self->rpc)
	{
		rpc_done(self->rpc);
		self->rpc = NULL;
	}
}

static void
checkpoint_main(void* arg)
{
	Checkpoint* self = arg;
	for (;;)
	{
		event_wait(&self->event, -1);
		checkpoint_create(self);
	}
}

void
checkpoint_init(Checkpoint* self, Db* db, Uuid* shard)
{
	self->active       = false;
	self->lsn          = 0;
	self->rpc          = NULL;
	self->coroutine_id = UINT64_MAX;
	self->shard        = shard;
	self->db           = db;

	locker_cache_init(&self->locker_cache);
	lock_init(&self->lock, &self->locker_cache);
	event_init(&self->event);
	snapshot_init(&self->snapshot);
}

void
checkpoint_free(Checkpoint* self)
{
	assert(! self->active);
	snapshot_free(&self->snapshot);
	locker_cache_free(&self->locker_cache);
}

void
checkpoint_start(Checkpoint* self)
{
	assert(self->coroutine_id == UINT64_MAX);
	self->coroutine_id = coroutine_create(checkpoint_main, self);
}

void
checkpoint_stop(Checkpoint* self)
{
	if (self->coroutine_id != UINT64_MAX)
	{
		coroutine_kill(self->coroutine_id);
		self->coroutine_id = UINT64_MAX;
	}
}

void
checkpoint(Checkpoint* self, uint64_t lsn, Rpc* rpc)
{
	assert(! self->active);
	self->active = true;
	self->lsn    = lsn;
	self->rpc    = rpc;
	event_signal(&self->event);
}
