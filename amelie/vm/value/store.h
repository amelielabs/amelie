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
	void (*free)(Store*);
	void (*encode)(Store*, Buf*);
	void (*export)(Store*, Buf*, Timezone*);
	bool (*in)(Store*, Value*);
};

static inline void
store_free(Store* self)
{
	self->free(self);
}

static inline void
store_encode(Store* self, Buf* buf)
{
	self->encode(self, buf);
}

static inline void
store_export(Store* self, Buf* buf, Timezone* timezone)
{
	self->export(self, buf, timezone);
}

static inline bool
store_in(Store* self, Value* value)
{
	return self->in(self, value);
}
