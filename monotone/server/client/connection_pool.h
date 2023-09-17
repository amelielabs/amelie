#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ConnectionPool ConnectionPool;

struct ConnectionPool
{
	Access access;
	Uri    uri;
	List   list;
	int    list_count;
};

static inline void
connection_pool_init(ConnectionPool* self)
{
	self->access     = ACCESS_UNDEF;
	self->list_count = 0;
	list_init(&self->list);
	uri_init(&self->uri);
}

static inline void
connection_pool_free(ConnectionPool* self)
{
	list_foreach_safe(&self->list) {
		auto conn = list_at(Connection, link);
		connection_free(conn);
	}
	self->list_count = 0;
	list_init(&self->list);
	uri_free(&self->uri);
}

static inline void
connection_pool_set(ConnectionPool* self, Access access, Str* spec)
{
	self->access = access;
	uri_set(&self->uri, true, spec);
}

static inline void
connection_pool_shutdown(ConnectionPool* self)
{
	list_foreach(&self->list) {
		auto conn = list_at(Connection, link);
		connection_close(conn);
	}
}

static inline Connection*
connection_pool_pop(ConnectionPool* self)
{
	Connection* conn = NULL;
	if (likely(self->list_count > 0))
	{
		auto first = list_pop(&self->list);
		self->list_count--;
		conn = container_of(first, Connection, link);
		connection_attach(conn);
	} else
	{
		conn = connection_create(self->access);
		guard(conn_guard, connection_free, conn);
		connection_set_uri(conn, true, &self->uri.uri);
		unguard(&conn_guard);
	}
	return conn;
}

static inline void
connection_pool_push(ConnectionPool* self, Connection* conn)
{
	connection_detach(conn);
	list_append(&self->list, &conn->link);
	self->list_count++;
}
