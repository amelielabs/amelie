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

typedef struct Resource Resource;

struct Resource
{
	Spinlock lock;
	int      lock_set[LOCK_MAX];
	List     lock_wait;
	int      lock_wait_count;
	char*    name;
};

static inline void
resource_init(Resource* self, char* name)
{
	self->name            = name;
	self->lock_wait_count = 0;
	list_init(&self->lock_wait);
	spinlock_init(&self->lock);
	memset(self->lock_set, 0, sizeof(self->lock_set));
}

static inline void
resource_free(Resource* self)
{
	spinlock_free(&self->lock);
}
