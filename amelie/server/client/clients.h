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

typedef struct Clients Clients;

struct Clients
{
	List list;
	int  list_count;
};

static inline void
clients_init(Clients* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
clients_free(Clients* self)
{
	unused(self);
	assert(self->list_count == 0);
}

static inline void
clients_add(Clients* self, Client* conn)
{
	list_append(&self->list, &conn->link);
	self->list_count++;
}

static inline void
clients_del(Clients* self, Client* conn)
{
	list_unlink(&conn->link);
	self->list_count--;
}

static inline void
clients_shutdown(Clients* self)
{
	while (self->list_count > 0)
	{
		auto conn = container_of(self->list.next, Client, link);
		coroutine_kill(conn->coroutine_id);
	}
}
