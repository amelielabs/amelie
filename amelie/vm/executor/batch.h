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
	Pipeline* pending;
	WriteList write;
};

static inline Dtr*
batch_at(Batch* self, int order)
{
	return ((Dtr**)self->list.start)[order];
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
batch_add(Batch* self, Dtr* dtr)
{
	buf_write(&self->list, &dtr, sizeof(Dtr**));
	self->list_count++;
}

static inline int
batch_sort_cb(const void* a, const void* b)
{
	return compare_uint64( (*(Dtr**)a)->id, (*(Dtr**)b)->id );
}

static inline void
batch_sort(Batch* self)
{
	// order dtrs by id
	if (self->list_count <= 1)
		return;
	qsort(self->list.start, self->list_count, sizeof(Dtr*),
	      batch_sort_cb);
}

hot static inline void
batch_add_partition(Batch* self, Ltr* ltr)
{
	// create a unique list of partitions
	auto pipeline = &ltr->part->pipeline;
	if (pipeline->pending)
		return;
	pipeline->pending = true;
	pipeline->pending_link = self->pending;
	self->pending = pipeline;
}

hot static inline void
batch_process(Batch* self)
{
	// process transaction
	for (auto it = 0; it < self->list_count; it++)
	{
		// handle aborts per partition
		auto dtr = batch_at(self, it);
		list_foreach(&dtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;

			// collect unique partitions across batch
			batch_add_partition(self, ltr);

			// abort transaction if its partition transaction id lower then
			// the partition abort id
			auto pipeline = &ltr->part->pipeline;
			auto pending  = &pipeline->pending_consensus;
			auto last     = &pipeline->consensus;
			if (pending->abort >= tr->id || last->abort >= tr->id)
				dtr_set_abort(dtr);
		}

		// sync metrics and prepare dtr for wal write
		auto write = &dtr->write;
		write_reset(write);
		write_begin(write);
		list_foreach(&dtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;

			// sync metrics
			auto pending = &ltr->part->pipeline.pending_consensus;
			if (dtr->abort)
			{
				// sync abort id
				if (tr->id > pending->abort)
					pending->abort = tr->id;
			} else
			{
				// sync commit id
				if (tr->id > pending->commit)
					pending->commit = tr->id;

				// add to the wal write list
				if (! tr_read_only(tr))
					write_add(write, &tr->log.write_log);
			}
		}
		if (write->header.count > 0)
			write_list_add(&self->write, write);
	}
}

hot static inline void
batch_abort(Batch* self)
{
	// abort all prepared transactions
	for (auto it = 0; it < self->list_count; it++)
	{
		auto dtr = batch_at(self, it);
		if (dtr->abort)
			continue;
		dtr_set_abort(dtr);

		// sync abort id
		list_foreach(&dtr->dispatch_mgr.ltrs)
		{
			auto ltr = list_at(Ltr, link);
			auto tr  = ltr->tr;
			if (! tr)
				continue;
			auto pending = &ltr->part->pipeline.pending_consensus;
			if (tr->id > pending->abort)
				pending->abort = tr->id;
		}
	}
	write_list_reset(&self->write);
}

hot static inline void
batch_wakeup(Batch* self)
{
	for (auto it = 0; it < self->list_count; it++)
	{
		auto dtr = batch_at(self, it);
		event_signal(&dtr->on_commit);
	}
}
