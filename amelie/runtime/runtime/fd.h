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

typedef struct Fd Fd;

struct Fd
{
	int  fd;
	int  mask;
	bool on_read;
	bool on_write;
	List link;
};

static inline void
fd_init(Fd* self)
{
	self->fd       = -1;
	self->mask     = 0;
	self->on_read  = false;
	self->on_write = false;
	list_init(&self->link);
}

static inline void
fd_reset(Fd* self)
{
	self->on_read  = false;
	self->on_write = false;
}
