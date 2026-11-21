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
	Buf       list_actions;
	int       count;
	int       count_actions;
	WriteList write;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline Gtr*
batch_at_actions(Batch* self, int order)
{
	return ((Gtr**)self->list_actions.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->count = 0;
	self->count_actions = 0;
	buf_init(&self->list);
	buf_init(&self->list_actions);
	write_list_init(&self->write);
}

static inline void
batch_free(Batch* self)
{
	buf_free(&self->list);
	buf_free(&self->list_actions);
}

static inline void
batch_reset(Batch* self)
{
	self->count = 0;
	self->count_actions = 0;
	buf_reset(&self->list);
	buf_reset(&self->list_actions);
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

	if (! buf_empty(&gtr->tr.log.actions))
	{
		buf_write(&self->list_actions, &gtr, sizeof(Gtr**));
		self->count_actions++;
	}
}

hot static inline void
batch_publish(Batch* self, Service* service)
{
	// publish
	for (auto it = 0; it < self->count_actions; it++)
	{
		auto gtr = batch_at_actions(self, it);

		Action* pos = (Action*)gtr->tr.log.actions.start;
		Action* end = (Action*)gtr->tr.log.actions.position;
		while (pos < end)
		{
			pos->lsn = gtr->write.record.lsn;
			service_add(service, pos);
			pos = (Action*)(pos->data + pos->data_size);
		}
	}

	service_resume(service);
}

hot static inline void
batch_complete(Batch* self, Service* service)
{
	// publish
	if (self->count_actions > 0)
		batch_publish(self, service);

	// wakeup transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);
		event_signal(&gtr->on_commit);
	}
}
