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

typedef void (*FdFunction)(Fd*);

struct Fd
{
	int        fd;
	int        mask;
	FdFunction on_read;
	void*      on_read_arg;
	FdFunction on_write;
	void*      on_write_arg;
};

static inline void
fd_init(Fd* self)
{
	memset(self, 0, sizeof(*self));
	self->fd = -1;
}
