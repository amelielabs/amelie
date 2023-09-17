#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ConnectionMgr ConnectionMgr;

struct ConnectionMgr
{
	List list;
	int  list_count;
};

static inline void
connection_mgr_init(ConnectionMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
connection_mgr_free(ConnectionMgr* self)
{
	assert(self->list_count == 0);
}

static inline void
connection_mgr_add(ConnectionMgr* self, Connection* conn)
{
	list_append(&self->list, &conn->link);
	self->list_count++;
}

static inline void
connection_mgr_del(ConnectionMgr* self, Connection* conn)
{
	list_unlink(&conn->link);
	self->list_count--;
}

static inline void
connection_mgr_shutdown(ConnectionMgr* self)
{
	while (self->list_count > 0)
	{
		auto conn = container_of(self->list.next, Connection, link);
		coroutine_kill(conn->coroutine_id);
	}
}

static inline Connection*
connection_mgr_find_by_uuid(ConnectionMgr* self, Uuid* uuid)
{
	list_foreach(&self->list)
	{
		auto conn = list_at(Connection, link);
		auto client_uuid = auth_get(&conn->auth, AUTH_UUID);
		Uuid id;
		uuid_from_string(&id, client_uuid);
		if (uuid_compare(&id, uuid))
			return conn;
	}
	return NULL;
}

static inline void
connection_mgr_shutdown_by_uuid(ConnectionMgr* self, Uuid* uuid)
{
	for (;;)
	{
		auto conn = connection_mgr_find_by_uuid(self, uuid);
		if (conn == NULL)
			break;
		coroutine_kill(conn->coroutine_id);
	}
}
