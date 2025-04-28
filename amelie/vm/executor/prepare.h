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
	int       list_count;
	List      list;
	Buf       cores;
	WriteList write;
};

static inline void
prepare_init(Prepare* self)
{
	self->id_max     = 0;
	self->list_count = 0;
	buf_init(&self->cores);
	list_init(&self->list);
	write_list_init(&self->write);
}

static inline void
prepare_free(Prepare* self)
{
	buf_free(&self->cores);
}

static inline void
prepare_reset(Prepare* self, CoreMgr* core_mgr)
{
	buf_reset(&self->cores);
	auto size = sizeof(Core*) * core_mgr->cores_count;
	buf_claim(&self->cores, size);
	memset(self->cores.start, 0, size);

	self->id_max     = 0;
	self->list_count = 0;
	list_init(&self->list);
	write_list_reset(&self->write);
}

hot static inline void
prepare_add(Prepare* self, Dtr* dtr)
{
	// add transaction to the list
	list_append(&self->list, &dtr->link_prepare);
	self->list_count++;

	// track max transaction id
	if (dtr->id > self->id_max)
		self->id_max = dtr->id;

	// create a list of active cores
	auto cores = (Core**)self->cores.start;
	auto dispatch_mgr = &dtr->dispatch_mgr;
	for (auto order = 0; order < dispatch_mgr->ctrs_count; order++)
	{
		auto ctr = dispatch_mgr_ctr(dispatch_mgr, order);
		if (ctr->state == CTR_NONE)
			continue;
		cores[order] = ctr->core;
	}
}
