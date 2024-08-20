
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
#include <amelie_server.h>

static void
server_listen_main(void* arg)
{
	ServerListen* listen = arg;
	Server*       self = listen->arg;

	Exception e;
	if (enter(&e))
	{
		// set listen address
		char addr_name[128];
		socket_getaddrname(listen->addr->ai_addr, addr_name, sizeof(addr_name), true, true);
		coroutine_set_name(am_self(), "listen %s", addr_name);

		// bind
		listen_start(&listen->listen, 4096, listen->addr->ai_addr);
		info("start");

		// process incoming connection
		for (;;)
		{
			// cancellation point
			int fd = listen_accept(&listen->listen);

			Client* client = NULL;
			Exception e;
			if (enter(&e))
			{
				// create new client
				client = client_create();
				client->arg = self;
				if (listen->tls)
					tcp_set_tls(&client->tcp, listen->tls_context);
				tcp_set_fd(&client->tcp, fd);
				fd = -1;

				// process client
				self->on_connect(self, client);
			}

			if (leave(&e))
			{
				if (client)
					client_free(client);
				if (fd != -1)
					socket_close(fd);
			}
		}
	}
	if (leave(&e))
	{ }

	listen_stop(&listen->listen);
}

static void
server_listen_add(Server* self, ServerConfig* config)
{
	if (str_empty(&config->host))
		error("server: listen[] <host> is not set"); 

	// listen '*'
	char* host = str_of(&config->host);
	if (str_size(&config->host) == 1 && *str_of(&config->host) == '*')
		host = NULL;

	// resolve
	resolve(global()->resolver, host, config->port, &config->host_addr);

	// configure and create server tls context
	if (config->tls)
	{
		auto context = &config->tls_context;

		// <directory>/certs
		char directory[PATH_MAX];
		snprintf(directory, sizeof(directory), "%s/certs", config_directory());

		// tls_cert
		auto tls_cert = &config()->tls_cert;
		if (! var_string_is_set(tls_cert))
			error("server: <tls_cert> is not defined"); 
		tls_context_set_path(context, TLS_FILE_CERT, directory,
		                     &tls_cert->string);

		// tls_key
		auto tls_key = &config()->tls_key;
		if (! var_string_is_set(tls_key))
			error("server: <tls_key> is not defined"); 
		tls_context_set_path(context, TLS_FILE_KEY, directory,
		                     &tls_key->string);

		// tls_ca
		auto tls_ca = &config()->tls_ca;
		if (var_string_is_set(tls_ca))
			tls_context_set_path(context, TLS_FILE_CA, directory,
			                     &tls_ca->string);

		// create tls context
		tls_context_create(context);
	}

	// foreach resolved address
	struct addrinfo* ai = config->host_addr;
	while (ai)
	{
		auto listen = server_listen_allocate(config);
		listen->addr        = ai;
		listen->tls         =  config->tls;
		listen->tls_context = &config->tls_context;
		list_append(&self->listen, &listen->link);
		self->listen_count++;
		ai = ai->ai_next;
	}
}

static void
server_listen(Server* self)
{
	if (! self->config_count)
		error("server: <listen> is not defined");

	// prepare listen objects according to the config
	list_foreach(&self->config)
	{
		auto config = list_at(ServerConfig, link);
		server_listen_add(self, config);
	}

	// start listen for incoming clients
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		listen->arg    = self;
		listen->worker = coroutine_create(server_listen_main, listen);
	}
}

static void
server_listen_set_uri(Server* self)
{
	// set listen_uri for cli
	auto listen_uri = &config()->listen_uri;
	auto first = container_of(self->config.next, ServerConfig, link);
	char uri[128];
	if (str_compare_raw(&first->host, "*", 1))
		snprintf(uri, sizeof(uri), "localhost:%d", (int)first->port);
	else
		snprintf(uri, sizeof(uri), "%.*s:%d", str_size(&first->host),
		         str_of(&first->host), (int)first->port);
	var_string_set_raw(listen_uri, uri, strlen(uri));
}

static void
server_listen_configure(Server* self)
{
	// listen is not defined or null
	auto listen = &config()->listen;
	if (! var_data_is_set(listen))
		return;
	auto pos = var_data_of(listen);
	if (data_is_null(pos))
		return;
	if (! data_is_array(pos))
		error("server: <listen> option expected to be array or null"); 

	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		auto config = server_config_read(&pos);
		list_append(&self->config, &config->link);
		self->config_count++;
	}
}

void
server_init(Server* self)
{
	self->on_connect     = NULL;
	self->on_connect_arg = NULL;
	self->listen_count   = 0;
	self->config_count   = 0;
	list_init(&self->listen);
	list_init(&self->config);
}

void
server_free(Server* self)
{
	list_foreach_safe(&self->listen) {
		auto listen = list_at(ServerListen, link);
		server_listen_free(listen);
	}
	list_foreach_safe(&self->config) {
		auto config = list_at(ServerConfig, link);
		server_config_free(config);
	}
}

void
server_start(Server*     self,
             ServerEvent on_connect,
             void*       on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;

	// listen for incoming clients
	server_listen_configure(self);
	server_listen(self);
	server_listen_set_uri(self);
}

void
server_stop(Server* self)
{
	// stop listen
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		coroutine_kill(listen->worker);
	}
}
