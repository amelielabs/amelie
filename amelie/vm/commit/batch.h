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
	Buf       list_publish;
	int       count;
	int       count_publish;
	Track*    pending;
	WriteList write;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline Gtr*
batch_at_publish(Batch* self, int order)
{
	return ((Gtr**)self->list_publish.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->pending       = NULL;
	self->count         = 0;
	self->count_publish = 0;
	buf_init(&self->list);
	buf_init(&self->list_publish);
	write_list_init(&self->write);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->list);
	buf_free(&self->list_publish);
}

static inline void
batch_reset(Batch* self)
{
	self->pending       = NULL;
	self->count         = 0;
	self->count_publish = 0;
	buf_reset(&self->list);
	buf_reset(&self->list_publish);
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

static inline void
batch_add_publish(Batch* self, Gtr* gtr)
{
	buf_write(&self->list_publish, &gtr, sizeof(Gtr**));
	self->count_publish++;
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
	for (auto it = 0; it < self->count; it++)
	{
		// handle aborts per partition
		auto gtr = batch_at(self, it);
		list_foreach(&gtr->dispatches.ltrs)
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
		list_foreach(&gtr->dispatches.ltrs)
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
			}
		}

		if (gtr->abort)
			continue;

		// add for wal write
		if (! gtr->program->ro)
			write_list_add(&self->write, write);
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
		list_foreach(&gtr->dispatches.ltrs)
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
batch_publish(Batch* self)
{
	// publish events to channels
	for (auto it = 0; it < self->count_publish; it++)
	{
		auto gtr = batch_at_publish(self, it);
		auto log = &gtr->tr.log;
		for (int pos = 0; pos < log->count; pos++)
		{
			auto op = log_of(log, pos);
			assert(op->cmd == LOG_PUBLISH);
			auto data = log->data.start + op->rel_data;
			stream_write(&channel_of(op->rel)->stream, data, op->rel_data_size);
		}
	}
}

hot static inline void
batch_complete(Batch* self)
{
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);

		// commit utility transaction
		tr_commit(&gtr->tr);

		// wakeup
		event_signal(&gtr->on_commit);
	}
}
