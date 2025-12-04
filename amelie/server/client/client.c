
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

#include <amelie_runtime>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>

Client*
client_create(void)
{
	Client* self;
	self = am_malloc(sizeof(Client));
	self->endpoint     = NULL;
	self->coroutine_id = UINT64_MAX;
	self->accepted     = false;
	self->auth         = false;
	self->arg          = NULL;
	msg_init(&self->msg, MSG_CLIENT);
	http_init(&self->request);
	http_init(&self->reply);
	tls_context_init(&self->tls_context);
	readahead_init(&self->readahead, &self->tcp, 16 * 1024);
	tcp_init(&self->tcp, &state()->sent_bytes.integer,
	         &state()->recv_bytes.integer);
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
	tls_context_free(&self->tls_context);
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
client_set_endpoint(Client* self, Endpoint* endpoint)
{
	self->endpoint = endpoint;
}

void
client_set_auth(Client* self, bool auth)
{
	self->auth = auth;
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
	self->accepted = true;

	// hello
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections)
	{
		char addr[128];
		tcp_getpeername(&self->tcp, addr, sizeof(addr));
		if (tls_is_set(&self->tcp.tls))
		{
			char tls_string[128];
			tls_explain(&self->tcp.tls, tls_string, sizeof(tls_string));
			info("connected from %s (%s)", addr, tls_string);
		} else {
			info("connected from %s", addr);
		}
	}
}

static void
client_connect_to_path(Client* self, Str* path)
{
	struct sockaddr_un addr_un;
	struct sockaddr*   addr = (struct sockaddr*)&addr_un;
	memset(&addr_un, 0, sizeof(addr_un));
	addr_un.sun_family = AF_UNIX;
	sfmt(addr_un.sun_path, sizeof(addr_un.sun_path) - 1, "%.*s/socket",
	     str_size(path), str_of(path));

	// connect
	tcp_connect(&self->tcp, addr);
	self->accepted = false;

	// connected
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %.*s/socket", str_size(path), str_of(path));
}

static void
client_connect_to(Client* self, Str* host, int port)
{
	// resolve host address
	struct addrinfo* addr = NULL;
	resolve(&runtime()->resolver, str_of(host), port, &addr);
	defer(freeaddrinfo, addr);

	// prepare for https connection
	auto endpoint = self->endpoint;
	if (endpoint->proto.integer == PROTO_HTTPS)
	{
		if (! tls_context_created(&self->tls_context))
		{
			tls_context_set(&self->tls_context, true,
			                opt_string_of(&endpoint->tls_cert),
			                opt_string_of(&endpoint->tls_key),
			                opt_string_of(&endpoint->tls_ca),
			                opt_string_of(&endpoint->tls_capath),
			                opt_string_of(&endpoint->tls_server));
			tls_context_create(&self->tls_context);
			tcp_set_tls(&self->tcp, &self->tls_context);
		}
	}

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

	// connected
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %s:%d", str_of(host), port);
}

void
client_connect(Client* self)
{
	// unix socket
	auto endpoint = self->endpoint;
	auto path = &endpoint->path;
	if (! opt_string_empty(path))
	{
		client_connect_to_path(self, &path->string);
		return;
	}

	// tcp connection
	auto host = &endpoint->host;
	if (opt_string_empty(host))
		error("client: host or unix path is not set");

	auto port = endpoint->port.integer;
	if (error_catch( client_connect_to(self, &host->string, port) ))
	{
		client_close(self);
		rethrow();
	}
}

void
client_close(Client* self)
{
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections && tcp_connected(&self->tcp))
	{
		if (self->accepted)
		{
			char addr[128];
			tcp_getpeername(&self->tcp, addr, sizeof(addr));
			info("disconnected from %s", addr);
		}
		else {
			auto endpoint = self->endpoint;
			info("disconnected from %s:%d",
			     str_of(opt_string_of(&endpoint->host)),
			     (int)endpoint->port.integer);
		}
	}
	tcp_close(&self->tcp);
	http_reset(&self->request);
	http_reset(&self->reply);
	readahead_reset(&self->readahead);
}
