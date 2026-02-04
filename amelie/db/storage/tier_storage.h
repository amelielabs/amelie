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

typedef struct TierStorage TierStorage;

struct TierStorage
{
	Str      name;
	Storage* storage;
	int      refs;
	List     link;
};

static inline TierStorage*
tier_storage_allocate(void)
{
	auto self = (TierStorage*)am_malloc(sizeof(TierStorage));
	self->storage = NULL;
	self->refs    = 0;
	str_init(&self->name);
	list_init(&self->link);
	return self;
}

static inline void
tier_storage_free(TierStorage* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
tier_storage_ref(TierStorage* self)
{
	self->refs++;
}

static inline void
tier_storage_unref(TierStorage* self)
{
	self->refs--;
	assert(self->refs >= 0);
}

static inline void
tier_storage_set_name(TierStorage* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline TierStorage*
tier_storage_copy(TierStorage* from)
{
	auto self = tier_storage_allocate();
	tier_storage_set_name(self, &from->name);
	return self;
}

static inline TierStorage*
tier_storage_read(uint8_t** pos)
{
	auto self = tier_storage_allocate();
	errdefer(tier_storage_free, self);
	json_read_string_copy(pos, &self->name);
	return self;
}

static inline void
tier_storage_write(TierStorage* self, Buf* buf)
{
	encode_string(buf, &self->name);
}
