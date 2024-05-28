
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>

Client*
client_create(void)
{
	Client* self;
	self = so_malloc(sizeof(Client));
	self->tls_context  = NULL;
	self->coroutine_id = UINT64_MAX;
	self->host         = NULL;
	self->arg          = NULL;
	uri_init(&self->uri);
	tcp_init(&self->tcp);
	list_init(&self->link);
	return self;
}

void
client_free(Client* self)
{
	client_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	uri_free(&self->uri);
	tcp_free(&self->tcp);
	so_free(self);
}

void
client_set_coroutine_name(Client* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(so_self(), "client %s", addr);
}

void
client_set_uri(Client* self, Str* spec)
{
	uri_set(&self->uri, spec);
}

void
client_attach(Client* self)
{
	assert(self->tcp.fd.fd != -1);
	tcp_attach(&self->tcp);
	self->coroutine_id = so_self()->id;
}

void
client_detach(Client* self)
{
	if (tcp_connected(&self->tcp))
		tcp_detach(&self->tcp);
	self->coroutine_id = UINT64_MAX;
}

void
client_accept(Client* self)
{
	// new client
	client_set_coroutine_name(self);

	// hello
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		log("connected");

	// handshake
	tcp_connect_fd(&self->tcp);
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
	guard(freeaddrinfo, addr);

	// todo: create tls context

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

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
		if (enter(&e))
			client_connect_to(self, host);

		if (leave(&e))
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
}
