
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>

Client*
client_create(void)
{
	Client* self;
	self = am_malloc(sizeof(Client));
	self->coroutine_id = UINT64_MAX;
	self->host         = NULL;
	self->auth         = false;
	self->arg          = NULL;
	http_init(&self->request);
	http_init(&self->reply);
	tls_context_init(&self->tls_context);
	readahead_init(&self->readahead, &self->tcp, 16 * 1024);
	uri_init(&self->uri);
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
client_set_remote(Client* self, Remote* remote)
{
	self->remote = remote;
	auto uri = remote_get(remote, REMOTE_URI);
	if (! str_empty(uri))
		uri_set(&self->uri, uri, false);
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
	snprintf(addr_un.sun_path, sizeof(addr_un.sun_path) - 1, "%.*s",
	         str_size(path), str_of(path));

	// connect
	tcp_connect(&self->tcp, addr);

	// connected
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %.*s", str_size(path), str_of(path));
}

static void
client_connect_to(Client* self, UriHost* host)
{
	// resolve host address
	struct addrinfo* addr = NULL;
	resolve(global()->resolver, str_of(&host->host), host->port, &addr);
	defer(freeaddrinfo, addr);

	// prepare for https connection
	if (self->uri.proto == URI_HTTPS) {
		if (! tls_context_created(&self->tls_context))
		{
			tls_context_create(&self->tls_context, true, self->remote);
			tcp_set_tls(&self->tcp, &self->tls_context);
		}
	}

	// connect
	tcp_connect(&self->tcp, addr->ai_addr);

	// connected
	self->host = host;
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections)
		info("connected to %s:%d", str_of(&host->host), host->port);
}

void
client_connect(Client* self)
{
	// unix socket
	auto path = remote_get(self->remote, REMOTE_PATH);
	if (! str_empty(path))
	{
		client_connect_to_path(self, path);
		return;
	}

	// tcp
	list_foreach(&self->uri.hosts)
	{
		auto host = list_at(UriHost, link);

		if (error_catch( client_connect_to(self, host) ))
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
	bool log_connections = opt_int_of(&config()->log_connections);
	if (log_connections && tcp_connected(&self->tcp))
	{
		if (self->host != NULL)
		{
			info("disconnected from %s:%d", str_of(&self->host->host),
			     self->host->port);
		}
		else
		{
			char addr[128];
			tcp_getpeername(&self->tcp, addr, sizeof(addr));
			info("disconnected from %s", addr);
		}
	}
	self->host = NULL;
	tcp_close(&self->tcp);
	http_reset(&self->request);
	http_reset(&self->reply);
	readahead_reset(&self->readahead);
}

hot void
client_execute(Client* self, Str* command)
{
	auto request = &self->request;
	auto reply   = &self->reply;

	// request
	http_write_request(request, "POST /");
	auto token = remote_get(self->remote, REMOTE_TOKEN);
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Length", "%d", str_size(command));
	http_write(request, "Content-Type", "text/plain");
	http_write_end(request);
	tcp_write_pair_str(&self->tcp, &request->raw, command);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &self->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &self->readahead, &reply->content);
}

hot void
client_import(Client* self, Str* path, Str* content_type, Str* content)
{
	auto request = &self->request;
	auto reply   = &self->reply;

	// request
	http_write_request(request, "POST %.*s", str_size(path), str_of(path));
	auto token = remote_get(self->remote, REMOTE_TOKEN);
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "%.*s", str_size(content_type), str_of(content_type));
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&self->tcp, &request->raw, content);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &self->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &self->readahead, &reply->content);
}
