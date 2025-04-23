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

typedef struct ClientMgr ClientMgr;

struct ClientMgr
{
	List list;
	int  list_count;
};

static inline void
client_mgr_init(ClientMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
client_mgr_free(ClientMgr* self)
{
	unused(self);
	assert(self->list_count == 0);
}

static inline void
client_mgr_add(ClientMgr* self, Client* conn)
{
	list_append(&self->list, &conn->link);
	self->list_count++;
	opt_int_add(&state()->connections, 1);
}

static inline void
client_mgr_del(ClientMgr* self, Client* conn)
{
	list_unlink(&conn->link);
	self->list_count--;
	opt_int_sub(&state()->connections, 1);
}

static inline void
client_mgr_shutdown(ClientMgr* self)
{
	while (self->list_count > 0)
	{
		auto conn = container_of(self->list.next, Client, link);
		coroutine_kill(conn->coroutine_id);
	}
}
