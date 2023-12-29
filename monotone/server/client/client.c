
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>

Client*
client_create(Access access)
{
	Client* self;
	self = mn_malloc(sizeof(Client));
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
client_free(Client* self)
{
	client_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	auth_free(&self->auth);
	uri_free(&self->uri);
	tcp_free(&self->tcp);
	mn_free(self);
}

void
client_set_access(Client* self, Access access)
{
	self->access = access;
}

void
client_set_mode(Client* self, AccessMode mode)
{
	self->mode = mode;
}

void
client_set_uri(Client* self, bool safe, Str* spec)
{
	uri_set(&self->uri, safe, spec); 
	auto access = uri_find(&self->uri, "mode", 4);
	if (access)
	{
		auto mode = access_mode_of(&access->value);
		client_set_mode(self, mode);
	} else {
		client_set_mode(self, ACCESS_MODE_ANY);
	}
}

void
client_set_coroutine_name(Client* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(mn_self(), "client %s", addr);
}

void
client_attach(Client* self)
{
	assert(self->tcp.fd.fd != -1);
	tcp_attach(&self->tcp);
	self->coroutine_id = mn_self()->id;
}

void
client_detach(Client* self)
{
	if (tcp_connected(&self->tcp))
		tcp_detach(&self->tcp);
	self->coroutine_id = UINT64_MAX;
}

void
client_accept(Client* self, UserCache* user_cache)
{
	// new client
	client_set_coroutine_name(self);

	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connected");

	// handshake
	tcp_connect_fd(&self->tcp);

	// authenticate
	auth_server(&self->auth, &self->tcp, user_cache);
	if (unlikely(! self->auth.complete))
		error("authentication failed");

	// update client type and access mode
	auto access = auth_get(&self->auth, AUTH_ACCESS);
	auto mode   = auth_get(&self->auth, AUTH_MODE);
	auto user   = auth_get(&self->auth, AUTH_USER);
	auto uuid   = auth_get(&self->auth, AUTH_UUID);

	client_set_access(self, access_of(access));
	client_set_mode(self, access_mode_of(mode));

	// hello
	if (log_connections)
		log("%.*s %.*s@%.*s",
		    str_size(access), str_of(access),
		    str_size(user), str_of(user),
		    str_size(uuid), str_of(uuid));
}

static void
client_connect_to(Client* self, UriHost* host)
{
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connected");

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

	// check client access
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
client_connect(Client* self)
{
	list_foreach(&self->uri.hosts)
	{
		auto host = list_at(UriHost, link);

		Exception e;
		if (try(&e))
			client_connect_to(self, host);

		if (catch(&e))
		{
			// reset and try next host
			client_close(self);

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
client_close(Client* self)
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
