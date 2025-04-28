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

typedef struct CoreMgr CoreMgr;

struct CoreMgr
{
	Core** cores;
	int    cores_count;
};

static inline void
core_mgr_init(CoreMgr* self)
{
	self->cores = NULL;
	self->cores_count = 0;
}

static inline void
core_mgr_free(CoreMgr* self)
{
	if (self->cores)
	{
		am_free(self->cores);
		self->cores = NULL;
	}
	self->cores_count = 0;
}

static inline void
core_mgr_allocate(CoreMgr* self, int count)
{
	auto size = sizeof(Core*) * count;
	self->cores = am_malloc(size);
	self->cores_count = count;
	memset(self->cores, 0, size);
}
