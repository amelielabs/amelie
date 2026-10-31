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

typedef struct Track Track;

struct Track
{
	Resource lock;
	Task*    compute;
};

static inline void
track_init(Track* self)
{
	self->compute = NULL;
	resource_init(&self->lock, "user");
}

static inline void
track_free(Track* self)
{
	resource_free(&self->lock);
}

static inline void
track_set_compute(Track* self, Task* task)
{
	self->compute = task;
}
