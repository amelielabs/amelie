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

typedef struct Store Store;
typedef struct Value Value;

struct Store
{
	void       (*free)(Store*);
	void       (*encode)(Store*, Timezone*, Buf*);
	void       (*export)(Store*, Timezone*, Buf*);
	atomic_u32 refs;
};

static inline void
store_init(Store* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
store_free(Store* self)
{
	if (atomic_u32_of(&self->refs) == 0)
	{
		self->free(self);
		return;
	}
	atomic_u32_dec(&self->refs);
}

static inline void
store_ref(Store* self)
{
	atomic_u32_inc(&self->refs);
}

static inline void
store_encode(Store* self, Timezone* tz, Buf* buf)
{
	self->encode(self, tz, buf);
}

static inline void
store_export(Store* self, Timezone* tz, Buf* buf)
{
	self->export(self, tz, buf);
}
