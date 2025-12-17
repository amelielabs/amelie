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
	uint64_t  commit_tsn;
	List      commit;
	int       commit_count;
	uint64_t  abort_tsn;
	List      abort;
	int       abort_count;
	Buf       cores;
	CoreMgr*  core_mgr;
	WriteList write;
};

static inline void
batch_init(Batch* self, CoreMgr* core_mgr)
{
	self->core_mgr     = core_mgr;
	self->commit_tsn   = 0;
	self->commit_count = 0;
	self->abort_tsn    = 0;
	self->abort_count  = 0;
	list_init(&self->commit);
	list_init(&self->abort);
	buf_init(&self->cores);
	write_list_init(&self->write);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->cores);
}

static inline void
batch_reset(Batch* self)
{
	buf_reset(&self->cores);
	auto size = sizeof(Core*) * self->core_mgr->cores_count;
	buf_emplace(&self->cores, size);
	memset(self->cores.start, 0, size);

	self->commit_tsn   = 0;
	self->commit_count = 0;
	self->abort_tsn    = 0;
	self->abort_count  = 0;
	list_init(&self->commit);
	list_init(&self->abort);
	write_list_reset(&self->write);
}

hot static inline void
batch_add_commit(Batch* self, Dtr* dtr)
{
	list_append(&self->commit, &dtr->link_batch);
	self->commit_count++;

	// track max tsn across commited transactions
	if (dtr->tsn > self->commit_tsn)
		self->commit_tsn = dtr->tsn;

	// create a list of active cores and wal write list
	auto cores = (Core**)self->cores.start;
	auto mgr   = &dtr->dispatch_mgr;
	auto write = &dtr->write;
	write_reset(write);
	write_begin(write, dtr->tsn);
	for (auto order = 0; order < mgr->ctrs_count; order++)
	{
		auto ctr = dispatch_mgr_ctr(mgr, order);
		if (! ctr->last)
			continue;
		cores[order] = ctr->core;
		if (ctr->tr == NULL || tr_read_only(ctr->tr))
			continue;
		write_add(write, &ctr->tr->log.write_log);
	}
	if (write->header.count > 0)
		write_list_add(&self->write, write);
}

hot static inline void
batch_add_abort(Batch* self, Dtr* dtr)
{
	list_append(&self->abort, &dtr->link_batch);
	self->abort_count++;

	// track max tsn_max across aborted transactions
	if (dtr->tsn_max > self->abort_tsn)
		self->abort_tsn = dtr->tsn_max;
}

hot static inline void
batch_add_abort_all(Batch* self)
{
	// move commit list to abort
	list_foreach_safe(&self->commit)
	{
		auto dtr = list_at(Dtr, link_batch);
		dtr_set_abort(dtr);
		list_init(&dtr->link_batch);
		batch_add_abort(self, dtr);
	}
	self->commit_tsn   = 0;
	self->commit_count = 0;
	list_init(&self->commit);
	write_list_reset(&self->write);
}

hot static inline void
batch_wakeup(Batch* self)
{
	list_foreach_safe(&self->commit)
	{
		auto dtr = list_at(Dtr, link_batch);
		event_signal(&dtr->on_commit);
	}
	list_foreach_safe(&self->abort)
	{
		auto dtr = list_at(Dtr, link_batch);
		event_signal(&dtr->on_commit);
	}
}
