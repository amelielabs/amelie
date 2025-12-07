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

typedef struct Prepare Prepare;

struct Prepare
{
	uint64_t  id_max;
	Dtr*      list;
	Dtr*      list_tail;
	int       list_count;
	Buf       cores;
	CoreMgr*  core_mgr;
	WriteList write;
};

static inline void
prepare_init(Prepare* self, CoreMgr* core_mgr)
{
	self->id_max     = 0;
	self->list       = NULL;
	self->list_tail  = NULL;
	self->list_count = 0;
	self->core_mgr   = core_mgr;
	buf_init(&self->cores);
	write_list_init(&self->write);
}

static inline void
prepare_free(Prepare* self)
{
	buf_free(&self->cores);
}

static inline void
prepare_reset(Prepare* self)
{
	buf_reset(&self->cores);
	auto size = sizeof(Core*) * self->core_mgr->cores_count;
	buf_emplace(&self->cores, size);
	memset(self->cores.start, 0, size);

	self->id_max     = 0;
	self->list       = NULL;
	self->list_tail  = NULL;
	self->list_count = 0;
	write_list_reset(&self->write);
}

hot static inline void
prepare_add(Prepare* self, Dtr* dtr)
{
	// add transaction to the list
	if (self->list)
		self->list_tail->link_queue = dtr;
	else
		self->list = dtr;
	self->list_tail = dtr;
	self->list_count++;

	// track max transaction id
	if (dtr->id > self->id_max)
		self->id_max = dtr->id;

	// create a list of active cores and wal write list
	auto cores = (Core**)self->cores.start;
	auto mgr   = &dtr->dispatch_mgr;
	auto write = &dtr->write;
	write_reset(write);
	write_begin(write);
	for (auto order = 0; order < mgr->ctrs_count; order++)
	{
		auto ctr = dispatch_mgr_ctr(mgr, order);
		if (ctr->state == CTR_NONE)
			continue;
		cores[order] = ctr->core;

		if (ctr->tr == NULL || tr_read_only(ctr->tr))
			continue;
		write_add(write, &ctr->tr->log.write_log);
	}
	if (write->header.count > 0)
		write_list_add(&self->write, write);
}
