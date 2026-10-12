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

typedef struct Retention Retention;

struct Retention
{
	Spinlock lock;
	uint64_t id;
	uint32_t count;
	uint32_t count_retention;
};

static inline void
retention_init(Retention* self)
{
	self->id              = 0;
	self->count           = 0;
	self->count_retention = UINT32_MAX;
	spinlock_init(&self->lock);
}

static inline void
retention_free(Retention* self)
{
	spinlock_free(&self->lock);
}

static inline void
retention_set(Retention* self, int64_t size)
{
	spinlock_lock(&self->lock);

	auto count = UINT32_MAX;
	if (size > 0)
	{
		auto page_size = 2 * 1024 * 1024;
		count = (size + page_size - 1) / page_size;
	}
	self->count_retention = count;

	spinlock_unlock(&self->lock);
}

static inline void
retention_follow(Retention* self, uint32_t id)
{
	spinlock_lock(&self->lock);
	self->count++;
	if (id > self->id)
		self->id = id + 1;
	spinlock_unlock(&self->lock);
}

static inline uint32_t
retention_attach(Retention* self)
{
	spinlock_lock(&self->lock);
	auto id = self->id++;
	self->count++;
	spinlock_unlock(&self->lock);
	return id;
}

static inline void
retention_detach(Retention* self)
{
	spinlock_lock(&self->lock);
	self->count--;
	assert(self->count >= 0);
	spinlock_unlock(&self->lock);
}

hot static inline uint32_t
retention_run(Retention* self)
{
	spinlock_lock(&self->lock);

	// everything < watermark considered for removal
	uint32_t watermark = 0;
	if (unlikely(self->count > self->count_retention))
	{
		assert(self->id >= self->count_retention);
		watermark = self->id - self->count_retention;
	}

	spinlock_unlock(&self->lock);
	return watermark;
}
