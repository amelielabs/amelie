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
	Buf    list;
	Buf    list_publish;
	int    count;
	int    count_publish;
	Writes writes;
};

static inline Gtr*
batch_at(Batch* self, int order)
{
	return ((Gtr**)self->list.start)[order];
}

static inline Write*
batch_at_publish(Batch* self, int order)
{
	return ((Write**)self->list_publish.start)[order];
}

static inline void
batch_init(Batch* self)
{
	self->count = 0;
	self->count_publish = 0;
	buf_init(&self->list);
	buf_init(&self->list_publish);
	writes_init(&self->writes);
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
	writes_reset(&self->writes);
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
	if (access_ro(gtr->access))
		return;

	// add write
	auto write = write_of(&gtr->write);
	writes_add(&self->writes, write);

	// prepare publish for write and send
	auto log = &gtr->tr.log;
	for (;;)
	{
		auto msg = log_publish_pop(log);
		if (! msg)
			break;
		write = (Write*)msg;
		writes_add(&self->writes, write);

		buf_write(&self->list_publish, &write, sizeof(Write**));
		self->count_publish++;
	}
}

hot static inline void
batch_publish(Batch* self)
{
	// publish
	for (auto it = 0; it < self->count_publish; it++)
	{
		auto write = batch_at_publish(self, it);
		auto db = (Db*)write->arg;
		task_send(db->compute, &write->msg);
	}
}

hot static inline void
batch_complete(Batch* self)
{
	// publish
	if (self->count_publish > 0)
		batch_publish(self);

	// wakeup transactions
	for (auto it = 0; it < self->count; it++)
	{
		auto gtr = batch_at(self, it);
		event_signal(&gtr->on_commit);
	}
}
