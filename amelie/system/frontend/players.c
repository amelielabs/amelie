
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
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>

Players*
players_allocate()
{
	auto self = (Players*)am_malloc(sizeof(Players));
	self->player       = NULL;
	self->player_count = 0;
	self->rr           = 0;
	player_sync_init(&self->sync);
	return self;
}

void
players_free(Players* self)
{
	assert(! self->player);
	am_free(self);
}

void
players_start(Players* self, Frontends* frontends)
{
	// prepare player clients
	self->player_count = 32;
	self->player = am_malloc(sizeof(Player) * self->player_count);

	auto sync  = &self->sync;
	auto fe_id = 0;
	for (auto i = 0; i < self->player_count; i++)
	{
		auto player = &self->player[i];
		auto fe = &frontends->workers[fe_id];
		fe_id++;
		if (fe_id >= frontends->workers_count)
			fe_id = 0;
		player_init(player, sync, fe);

		// deploy
		frontend_add(fe, &player->msg);
	}

	// sync gtrs and commit recover state to the current lsn
	gtrs_sync(share()->gtrs);
	commit_sync(share()->commit);
}

void
players_stop(Players* self)
{
	if (! self->player)
		return;

	// wait for completion
	player_sync(&self->sync);

	// stop player clients
	for (auto i = 0; i < self->player_count; i++)
	{
		auto player = &self->player[i];
		frontend_add(player->fe, &player->msg_stop);
	}

	// wait for stop
	player_sync_shutdown(&self->sync, self->player_count);

	// cleanup
	for (auto i = 0; i < self->player_count; i++)
	{
		auto player = &self->player[i];
		player_free(player);
	}
	am_free(self->player);

	self->player = NULL;
	self->player_count = 0;
}

void
players_sync(Players* self)
{
	player_sync(&self->sync);

	// sync gtrs and commit recover state to the current lsn
	gtrs_sync(share()->gtrs);
	commit_sync(share()->commit);
}

static inline Player*
players_next(Players* self)
{
	if (self->rr == self->player_count)
		self->rr = 0;
	auto rr = self->rr++;
	return &self->player[rr];
}

void
players_execute(Players* self, RecordMsg* msg)
{
	// ddl (wait for completion)
	auto sync = &self->sync;
	auto sync_after = false;
	if (unlikely(msg->record->flags & RECORD_UTILITY))
	{
		players_sync(self);
		sync_after = true;
	}

	// send to the next frontend player client
	auto player = players_next(self);
	msg->arg = player;
	frontend_add(player->fe, &msg->msg);
	player_sync_add(sync);

	// sync
	uint64_t total = atomic_u64_of(&sync->total);
	if (sync_after || !(total % 10000))
		players_sync(self);
}
