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

typedef struct Stream Stream;

struct Stream
{
	Str       user;
	Str       name;
	Uuid      id;
	CdcCursor cursor;
	CdcSlot   slot;
	List      link;
};

static inline Stream*
stream_allocate(void)
{
	auto self = (Stream*)am_malloc(sizeof(Stream));
	str_init(&self->user);
	str_init(&self->name);
	uuid_init(&self->id);
	cdc_cursor_init(&self->cursor);
	cdc_slot_init(&self->slot);
	list_init(&self->link);
	return self;
}

static inline void
stream_free(Stream* self)
{
	str_free(&self->user);
	str_free(&self->name);
	am_free(self);
}

static inline void
stream_set_user(Stream* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
stream_set_name(Stream* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
stream_set_id(Stream* self, Uuid* id)
{
	self->id = *id;
}
