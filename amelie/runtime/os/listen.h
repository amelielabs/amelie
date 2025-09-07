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

typedef struct Listen Listen;

struct Listen
{
	int fd;
};

void listen_init(Listen*);
void listen_start(Listen*, int, struct sockaddr*);
void listen_stop(Listen*);

static inline int
listen_accept(Listen* self)
{
	return io_accept(self->fd, NULL, NULL, 0);
}
