#pragma once

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

typedef struct Batch Batch;

struct Batch
{
	Buf       list;
	int       count;
	Track*    pending;
	WriteList write;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->pending = NULL;
	self->count   = 0;
	buf_init(&self->list);
	write_list_init(&self->write);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->list);
}

static inline void
batch_reset(Batch* self)
{
	self->pending = NULL;
	self->count   = 0;
	buf_reset(&self->list);
	write_list_reset(&self->write);
}

static inline bool
batch_empty(Batch* self)
{
	return !self->count;
}

static inline void
batch_add(Batch* self, Gtr* gtr)
{
	buf_write(&self->list, &gtr, sizeof(Gtr**));
	self->count++;
}

hot static inline void
batch_add_user(Batch* self, Peer* peer)
{
	// create a unique list of users
	auto track = &peer->user->track;
	if (track->pending)
		return;
	track->pending = true;
	track->pending_link = self->pending;
	self->pending = track;
}

hot static inline void
batch_process(Batch* self)
{
	// process transaction
	for (auto it = 0; it < self->count; it++)
	{
		// handle aborts per user
		auto gtr = batch_at(self, it);
		for (auto i = 0; i < gtr->peers.count; i++)
		{
			auto peer = peers_at(&gtr->peers, i);

			// collect unique users across batch
			batch_add_user(self, peer);

			// abort transaction if its local transaction id lower
			// then the abort id
			auto track   = &peer->user->track;
			auto pending = &track->pending_consensus;
			auto last    = &track->consensus_commit_last;
			auto tr      = peer->tr;
			if (pending->abort >= tr->id || last->abort >= tr->id)
				gtr_set_abort(gtr);
		}

		// sync metrics and prepare gtr for wal write
		for (auto i = 0; i < gtr->peers.count; i++)
		{
			auto peer = peers_at(&gtr->peers, i);
			auto tr   = peer->tr;

			// sync metrics
			auto pending = &peer->user->track.pending_consensus;
			if (gtr->abort)
			{
				// sync abort id
				if (tr->id > pending->abort)
					pending->abort = tr->id;
			} else
			{
				// sync commit id
				if (tr->id > pending->commit)
					pending->commit = tr->id;
			}
		}
		if (gtr->abort)
			continue;

		// add for wal write
		if (! access_ro(&gtr->program->access))
			write_list_add(&self->write, &gtr->write);
	}
}

hot static inline void
batch_abort(Batch* self)
{
	// abort all prepared transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);

		// abort utility transaction
		tr_abort(&gtr->tr);

		if (gtr->abort)
			continue;
		gtr_set_abort(gtr);

		// sync abort id
		for (auto i = 0; i < gtr->peers.count; i++)
		{
			auto peer = peers_at(&gtr->peers, i);
			auto tr   = peer->tr;
			auto pending = &peer->user->track.pending_consensus;
			if (tr->id > pending->abort)
				pending->abort = tr->id;
		}
	}

	write_list_reset(&self->write);
}

hot static inline void
batch_complete(Batch* self)
{
	// group completion

	// apply pending metrics
	auto ref = self->pending;
	while (ref)
	{
		ref->consensus_commit_last = ref->pending_consensus;
		consensus_atomic_write(&ref->consensus_commit, &ref->consensus_commit_last);
		auto next = ref->pending_link;
		ref->pending_link = NULL;
		ref->pending      = false;
		ref = next;
	}

	// wakeup transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);

		// commit utility transaction
		tr_commit(&gtr->tr);

		// wakeup
		event_signal(&gtr->on_commit);
	}
}
