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
	WriteList write;
};

static inline void
batch_init(Batch* self)
{
	self->commit_tsn   = 0;
	self->commit_count = 0;
	self->abort_tsn    = 0;
	self->abort_count  = 0;
	list_init(&self->commit);
	list_init(&self->abort);
	write_list_init(&self->write);
}

static inline void
batch_free(Batch* self)
{
	unused(self);
}

static inline void
batch_reset(Batch* self)
{
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

	// sync max tsn across commited transactions
	if (dtr->tsn > self->commit_tsn)
		self->commit_tsn = dtr->tsn;

	// prepare wal write list
	auto mgr   = &dtr->dispatch_mgr;
	auto write = &dtr->write;
	write_reset(write);
	write_begin(write, dtr->tsn);
	list_foreach_safe(&mgr->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->tr && !tr_read_only(ltr->tr))
			write_add(write, &ltr->tr->log.write_log);
	}
	if (write->header.count > 0)
		write_list_add(&self->write, write);
}

hot static inline void
batch_add_abort(Batch* self, Dtr* dtr)
{
	list_append(&self->abort, &dtr->link_batch);
	self->abort_count++;

	// if aborted transaction has no observed tsn_max
	// forcing it to abort self by setting tsn_max = tsn
	//
	if (dtr->tsn_max == 0)
		dtr_sync_tsn_max(dtr, dtr->tsn);

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
