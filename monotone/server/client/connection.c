
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>

Connection*
connection_create(Access access)
{
	Connection* self;
	self = mn_malloc(sizeof(Connection));
	self->access       = access;
	self->mode         = ACCESS_MODE_ANY;
	self->host         = NULL;
	self->tls_context  = NULL;
	self->coroutine_id = UINT64_MAX;
	self->arg          = NULL;
	tcp_init(&self->tcp);
	auth_init(&self->auth);
	uri_init(&self->uri);
	list_init(&self->link);
	return self;
}

void
connection_free(Connection* self)
{
	connection_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	auth_free(&self->auth);
	uri_free(&self->uri);
	tcp_free(&self->tcp);
	mn_free(self);
}

void
connection_set_access(Connection* self, Access access)
{
	self->access = access;
}

void
connection_set_mode(Connection* self, AccessMode mode)
{
	self->mode = mode;
}

void
connection_set_uri(Connection* self, bool safe, Str* spec)
{
	uri_set(&self->uri, safe, spec); 
	auto access = uri_find(&self->uri, "mode", 4);
	if (access)
	{
		auto mode = access_mode_of(&access->value);
		connection_set_mode(self, mode);
	} else {
		connection_set_mode(self, ACCESS_MODE_ANY);
	}
}

void
connection_set_coroutine_name(Connection* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(mn_self(), "connection %s", addr);
}

void
connection_attach(Connection* self)
{
	assert(self->tcp.fd.fd != -1);
	tcp_attach(&self->tcp);
	self->coroutine_id = mn_self()->id;
}

void
connection_detach(Connection* self)
{
	if (tcp_connected(&self->tcp))
		tcp_detach(&self->tcp);
	self->coroutine_id = UINT64_MAX;
}

void
connection_accept(Connection* self, UserMgr* user_mgr)
{
	// new connection
	connection_set_coroutine_name(self);

	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connected");

	// handshake
	tcp_connect_fd(&self->tcp);

	// authenticate
	auth_server(&self->auth, &self->tcp, user_mgr);
	if (unlikely(! self->auth.complete))
		error("authentication failed");

	// update connection type and access mode
	auto access = auth_get(&self->auth, AUTH_ACCESS);
	auto mode   = auth_get(&self->auth, AUTH_MODE);
	auto user   = auth_get(&self->auth, AUTH_USER);
	auto uuid   = auth_get(&self->auth, AUTH_UUID);

	connection_set_access(self, access_of(access));
	connection_set_mode(self, access_mode_of(mode));

	// hello
	if (log_connections)
		log("%.*s %.*s@%.*s",
		    str_size(access), str_of(access),
		    str_size(user), str_of(user),
		    str_size(uuid), str_of(uuid));
}

static void
connection_connect_to(Connection* self, UriHost* host)
{
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connecting to %s:%d", str_of(&host->host), host->port);

	// resolve host address
	struct addrinfo* addr = NULL;
	resolve(global()->resolver, str_of(&host->host), host->port, &addr);
	guard(addr_guard, freeaddrinfo, addr);

	// todo: create tls context

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

	// set authentication
	assert(self->access != ACCESS_UNDEF);
	Str access;
	Str access_mode;
	access_str(self->access, &access);
	access_mode_str(self->mode, &access_mode);
	auth_set(&self->auth, AUTH_ACCESS, &access);
	auth_set(&self->auth, AUTH_MODE, &access_mode);
	auth_set(&self->auth, AUTH_USER, &self->uri.user);
	auth_set(&self->auth, AUTH_SECRET, &self->uri.password);

	// authenticate
	auth_client(&self->auth, &self->tcp);
	if (unlikely(! self->auth.complete))
		error("authentication failed");

	// check connection access
	if (self->auth.ro)
	{
		if (self->mode == ACCESS_MODE_RW)
			error("host %s:%d is read-only, closing", str_of(&host->host),
			      host->port);
	}

	// connected
	self->host = host;
	if (log_connections)
		log("connected to %s:%d", str_of(&host->host), host->port);
}

void
connection_connect(Connection* self)
{
	list_foreach(&self->uri.hosts)
	{
		auto host = list_at(UriHost, link);

		Exception e;
		if (try(&e))
			connection_connect_to(self, host);

		if (catch(&e))
		{
			// reset and try next host
			connection_close(self);

			// generate only one error for a single host
			if (self->uri.hosts_count == 1)
				rethrow();
		}
		if (self->host)
			break;
	}

	if (self->host == NULL)
		error("failed to connect to any host");
}

void
connection_close(Connection* self)
{
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections && tcp_connected(&self->tcp))
	{
		if (self->host != NULL)
			log("disconnected from %s:%d", str_of(&self->host->host),
			    self->host->port);
		else
			log("disconnected");
	}
	self->host = NULL;
	tcp_close(&self->tcp);
	auth_reset(&self->auth);
}
