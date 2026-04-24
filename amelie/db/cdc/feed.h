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

typedef struct Feed Feed;

struct Feed
{
	Str       user;
	Str       name;
	Uuid      id;
	CdcCursor cursor;
	CdcSlot   slot;
	List      link;
};

static inline Feed*
feed_allocate(void)
{
	auto self = (Feed*)am_malloc(sizeof(Feed));
	str_init(&self->user);
	str_init(&self->name);
	uuid_init(&self->id);
	cdc_cursor_init(&self->cursor);
	cdc_slot_init(&self->slot);
	list_init(&self->link);
	return self;
}

static inline void
feed_free(Feed* self)
{
	str_free(&self->user);
	str_free(&self->name);
	am_free(self);
}

static inline void
feed_set_user(Feed* self, Str* value)
{
	str_free(&self->user);
	str_copy(&self->user, value);
}

static inline void
feed_set_name(Feed* self, Str* value)
{
	str_free(&self->name);
	str_copy(&self->name, value);
}

static inline void
feed_set_id(Feed* self, Uuid* id)
{
	self->id = *id;
}
