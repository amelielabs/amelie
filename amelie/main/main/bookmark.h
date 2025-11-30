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

typedef struct Bookmark Bookmark;

struct Bookmark
{
	Endpoint endpoint;
	List     link;
};

static inline Bookmark*
bookmark_allocate(void)
{
	Bookmark* self;
	self = am_malloc(sizeof(*self));
	endpoint_init(&self->endpoint);
	list_init(&self->link);
	return self;
}

static inline void
bookmark_free(Bookmark* self)
{
	endpoint_free(&self->endpoint);
	am_free(self);
}

static inline void
bookmark_set_endpoint(Bookmark* self, Endpoint* endpoint)
{
	endpoint_copy(&self->endpoint, endpoint);
}

static inline Bookmark*
bookmark_copy(Bookmark* self)
{
	auto copy = bookmark_allocate();
	bookmark_set_endpoint(copy, &self->endpoint);
	return copy;
}

static inline Bookmark*
bookmark_read(uint8_t** pos)
{
	auto self = bookmark_allocate();
	errdefer(bookmark_free, self);
	endpoint_read(&self->endpoint, pos);
	return self;
}

static inline void
bookmark_write(Bookmark* self, Buf* buf)
{
	endpoint_write(&self->endpoint, buf);
}
