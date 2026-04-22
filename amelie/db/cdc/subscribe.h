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

typedef struct Subscribe Subscribe;

struct Subscribe
{
	Str       user;
	Str       name;
	Uuid      id;
	CdcCursor cursor;
	CdcSlot   slot;
	List      link;
};

static inline Subscribe*
subscribe_allocate(void)
{
	auto self = (Subscribe*)am_malloc(sizeof(Subscribe));
	str_init(&self->user);
	str_init(&self->name);
	uuid_init(&self->id);
	cdc_cursor_init(&self->cursor);
	cdc_slot_init(&self->slot);
	list_init(&self->link);
	return self;
}

static inline void
subscribe_free(Subscribe* self)
{
	str_free(&self->user);
	str_free(&self->name);
	am_free(self);
}

static inline void
subscribe_set_user(Subscribe* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
subscribe_set_name(Subscribe* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}
