#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
