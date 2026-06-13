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
	int       list_count;
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
	self->pending    = NULL;
	self->list_count = 0;
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
	self->pending    = NULL;
	self->list_count = 0;
	buf_reset(&self->list);
	write_list_reset(&self->write);
}

static inline bool
batch_empty(Batch* self)
{
	return !self->list_count;
}

static inline void
batch_add(Batch* self, Gtr* gtr)
{
	buf_write(&self->list, &gtr, sizeof(Gtr**));
	self->list_count++;
}

hot static inline void
batch_add_partition(Batch* self, Ltr* ltr)
{
	// create a unique list of partitions
	auto track = &ltr->part->track;
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
	for (auto it = 0; it < self->list_count; it++)
	{
		// handle aborts per partition
		auto gtr = batch_at(self, it);
		list_foreach(&gtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;

			// collect unique partitions across batch
			batch_add_partition(self, ltr);

			// abort transaction if its partition transaction id lower then
			// the partition abort id
			auto track   = &ltr->part->track;
			auto pending = &track->pending_consensus;
			auto last    = &track->consensus;
			if (pending->abort >= tr->id || last->abort >= tr->id)
				gtr_set_abort(gtr);
		}

		// sync metrics and prepare gtr for wal write
		auto write = &gtr->write;

		list_foreach(&gtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;

			// sync metrics
			auto pending = &ltr->part->track.pending_consensus;
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

				// collect cdc per ltr
				if (! log_cdc_empty(&tr->log.cdc))
					list_append(&gtr->write_cdc, &tr->log.cdc.link);
			}
		}

		// collect cdc
		if (! log_cdc_empty(&gtr->tr.log.cdc))
			list_append(&gtr->write_cdc, &gtr->tr.log.cdc.link);

		if (! gtr->program->ro)
			write_list_add(&self->write, write);
	}
}

hot static inline void
batch_abort(Batch* self)
{
	// abort all prepared transactions
	for (auto it = 0; it < self->list_count; it++)
	{
		auto gtr = batch_at(self, it);

		// abort utility transaction
		tr_abort(&gtr->tr);

		if (gtr->abort)
			continue;
		gtr_set_abort(gtr);

		// sync abort id
		list_foreach(&gtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;
			auto pending = &ltr->part->track.pending_consensus;
			if (tr->id > pending->abort)
				pending->abort = tr->id;
		}
	}
	write_list_reset(&self->write);
}

hot static inline void
batch_complete(Batch* self, Cdc* cdc)
{
	for (auto it = 0; it < self->list_count; it++)
	{
		auto gtr = batch_at(self, it);

		// publish cdc events
		if (! list_empty(&gtr->write_cdc))
		{
			auto lsn = gtr->write.record.lsn;
			cdc_write_batch(cdc, lsn, &gtr->write_cdc);
		}

		// commit utility transaction
		tr_commit(&gtr->tr);

		// wakeup
		event_signal(&gtr->on_commit);
	}
}
