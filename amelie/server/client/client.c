
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>

Client*
client_create(void)
{
	Client* self;
	self = am_malloc(sizeof(Client));
	self->tls_context  = NULL;
	self->coroutine_id = UINT64_MAX;
	self->host         = NULL;
	self->arg          = NULL;
	http_init(&self->request);
	http_init(&self->reply);
	readahead_init(&self->readahead, &self->tcp, 16 * 1024);
	uri_init(&self->uri);
	tcp_init(&self->tcp);
	list_init(&self->link);
	return self;
}

void
client_free(Client* self)
{
	http_free(&self->request);
	http_free(&self->reply);
	readahead_free(&self->readahead);
	client_close(self);
	if (self->tls_context)
		tls_context_free(self->tls_context);
	uri_free(&self->uri);
	tcp_free(&self->tcp);
	am_free(self);
}

void
client_set_coroutine_name(Client* self)
{
	char addr[128];
	tcp_getpeername(&self->tcp, addr, sizeof(addr));
	coroutine_set_name(am_self(), "client %s", addr);
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
	self->coroutine_id = am_self()->id;
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

	// handshake
	tcp_connect_fd(&self->tcp);

	// hello
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
	{
		if (tls_is_set(&self->tcp.tls))
		{
			char tls_string[128];
			tls_explain(&self->tcp.tls, tls_string, sizeof(tls_string));
			info("connected (%s)", tls_string);
		} else {
			info("connected");
		}
	}
}

static void
client_connect_to(Client* self, UriHost* host)
{
	// resolve host address
	struct addrinfo* addr = NULL;
	resolve(global()->resolver, str_of(&host->host), host->port, &addr);
	guard(freeaddrinfo, addr);

	// todo: create tls context

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

	// connected
	self->host = host;
	bool log_connections = var_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %s:%d", str_of(&host->host), host->port);
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
			info("disconnected from %s:%d", str_of(&self->host->host),
			     self->host->port);
		else
			info("disconnected");
	}
	self->host = NULL;
	tcp_close(&self->tcp);
	http_reset(&self->request);
	http_reset(&self->reply);
	readahead_reset(&self->readahead);
}
