
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
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>

ReplayMgr*
replay_mgr_allocate()
{
	auto self = (ReplayMgr*)am_malloc(sizeof(ReplayMgr));
	self->replay       = NULL;
	self->replay_count = 0;
	self->replay_id    = 0;
	self->rr           = 0;
	replay_sync_init(&self->sync);
	return self;
}

void
replay_mgr_free(ReplayMgr* self)
{
	assert(! self->replay);
	am_free(self);
}

void
replay_mgr_start(ReplayMgr* self, FrontendMgr* mgr)
{
	// prepare replay clients
	self->replay_count = 32;
	self->replay = am_malloc(sizeof(Replay) * self->replay_count);

	auto sync  = &self->sync;
	auto fe_id = 0;
	for (auto i = 0; i < self->replay_count; i++)
	{
		auto replay = &self->replay[i];
		auto fe = &mgr->workers[fe_id];
		fe_id++;
		if (fe_id >= mgr->workers_count)
			fe_id = 0;
		replay_init(replay, sync, fe);

		// deploy
		frontend_add(fe, &replay->msg);
	}

	// set starting reply id
	self->replay_id = gtr_mgr_recover_id(share()->gtr_mgr);
}

void
replay_mgr_stop(ReplayMgr* self)
{
	if (! self->replay)
		return;

	// wait for completion
	replay_sync(&self->sync);

	// stop replay clients
	for (auto i = 0; i < self->replay_count; i++)
	{
		auto replay = &self->replay[i];
		frontend_add(replay->fe, &replay->msg_stop);
	}

	// wait for stop
	replay_sync_shutdown(&self->sync, self->replay_count);

	// cleanup
	for (auto i = 0; i < self->replay_count; i++)
	{
		auto replay = &self->replay[i];
		replay_free(replay);
	}
	am_free(self->replay);

	self->replay = NULL;
	self->replay_count = 0;
}

void
replay_mgr_sync(ReplayMgr* self)
{
	replay_sync(&self->sync);
}

static inline Replay*
replay_mgr_next(ReplayMgr* self)
{
	if (self->rr == self->replay_count)
		self->rr = 0;
	auto rr = self->rr++;
	return &self->replay[rr];
}

void
replay_mgr_execute(ReplayMgr* self, RecordMsg* msg)
{
	// ddl (wait for completion)
	auto sync = &self->sync;
	auto sync_after = false;
	if (unlikely(msg->record->flags & RECORD_UTILITY))
	{
		replay_sync(sync);
		sync_after = true;
	} else
	{
		// set replay id
		msg->record_id = self->replay_id++;
	}

	// send to the next frontend replay client
	auto replay = replay_mgr_next(self);
	msg->arg = replay;
	frontend_add(replay->fe, &msg->msg);
	replay_sync_add(sync);

	// sync
	uint64_t total = atomic_u64_of(&sync->total);
	if (sync_after || !(total % 10000))
		replay_sync(sync);
}
