#pragma once

//
// indigo
//
// SQL OLTP database
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
	assert(self->list_count == 0);
}

static inline void
client_mgr_add(ClientMgr* self, Client* conn)
{
	list_append(&self->list, &conn->link);
	self->list_count++;
}

static inline void
client_mgr_del(ClientMgr* self, Client* conn)
{
	list_unlink(&conn->link);
	self->list_count--;
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

static inline Client*
client_mgr_find_by_uuid(ClientMgr* self, Uuid* uuid)
{
	list_foreach(&self->list)
	{
		auto conn = list_at(Client, link);
		auto client_uuid = auth_get(&conn->auth, AUTH_UUID);
		Uuid id;
		uuid_from_string(&id, client_uuid);
		if (uuid_compare(&id, uuid))
			return conn;
	}
	return NULL;
}

static inline void
client_mgr_shutdown_by_uuid(ClientMgr* self, Uuid* uuid)
{
	for (;;)
	{
		auto conn = client_mgr_find_by_uuid(self, uuid);
		if (conn == NULL)
			break;
		coroutine_kill(conn->coroutine_id);
	}
}
