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
	self->count = 0;
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
	self->count = 0;
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

	if (! access_ro(gtr->access))
		write_list_add(&self->write, &gtr->write);

	if (gtr->tr.log.count_publish)
	{
		buf_write(&self->list_publish, &gtr, sizeof(Gtr**));
		self->count_publish++;
	}
}

hot static inline void
batch_complete(Batch* self)
{
	// publish
	for (auto it = 0; it < self->count_publish; it++)
	{
		auto gtr = batch_at_publish(self, it);
		Msg* msg;
		while ((msg = log_publish_pop(&gtr->tr.log)))
		{
			auto publish = publish_of(msg);
			publish->lsn = gtr->write.record.lsn;

			auto db = (Db*)publish->db_arg;
			task_send(db->compute, msg);
		}
	}

	// wakeup transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);
		event_signal(&gtr->on_commit);
	}
}
