
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
#include <amelie_server.h>

static inline void
server_accept_client(Server* self, ServerListen* listen)
{
	ServerConfig* config = listen->config;

	// cancellation point
	int fd = listen_accept(&listen->listen);

	Client* client = NULL;
	auto on_error = error_catch
	(
		// create new client
		client = client_create();
		client_set_auth(client, config->auth);
		if (config->tls)
			tcp_set_tls(&client->tcp, &self->tls);
		tcp_set_fd(&client->tcp, fd);
		fd = -1;

		// process client
		self->on_connect(self, client);
	);

	if (on_error)
	{
		if (client)
			client_free(client);
		if (fd != -1)
			socket_close(fd);
	}
}

static void
server_accept_main(void* arg)
{
	ServerListen* listen = arg;
	Server*       self   = listen->arg;

	coroutine_set_name(am_self(), "accept");
	coroutine_set_cancel_log(am_self(), false);

	// process incoming connection
	error_catch
	(
		for (;;)
			server_accept_client(self, listen);
	);

	// remove unix socket after use
	if (! listen->addr)
		vfs_unlink(str_of(&listen->addr_name));

	listen_stop(&listen->listen);
}

static void
server_accept(Server* self)
{
	// start accepting incoming clients
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		listen->arg    = self;
		listen->worker = coroutine_create(server_accept_main, listen);
	}
}

static void
server_listen(ServerListen* listen)
{
	ServerConfig* config = listen->config;

	// start listen for incoming clients
	auto               is_unixsocket = !listen->addr;
	char               addr_name[PATH_MAX];
	struct sockaddr_un addr_un;
	struct sockaddr*   addr;

	// set listen address
	if (is_unixsocket)
	{
		auto path = &config->path;
		memset(&addr_un, 0, sizeof(addr_un));
		addr_un.sun_family = AF_UNIX;
		if (*str_of(path) == '/')
			snprintf(addr_name, sizeof(addr_name), "%.*s",
			         str_size(path), str_of(path));
		else
			snprintf(addr_name, sizeof(addr_name), "%s/%.*s",
			         state_directory(),
			         str_size(path), str_of(path));
		snprintf(addr_un.sun_path, sizeof(addr_un.sun_path) - 1, "%s", addr_name);
		vfs_unlink(addr_name);
		addr = (struct sockaddr*)&addr_un;
	} else
	{
		addr = listen->addr->ai_addr;
		socket_getaddrname(addr, addr_name, sizeof(addr_name), true, true);
	}

	// set address name
	str_dup_cstr(&listen->addr_name, addr_name);

	info("listening on '%.*s'", str_size(&listen->addr_name),
	     str_of(&listen->addr_name));

	// bind
	listen_start(&listen->listen, 4096, addr);

	// change unix socket mode
	if (is_unixsocket)
	{
		auto rc = chmod(addr_name, config->path_mode);
		if (rc == -1)
			error_system();
	}
}

static void
server_add(Server* self, ServerConfig* config)
{
	// ensure certificate has been loaded
	if (config->tls && !tls_context_created(&self->tls))
		error("server: listen[] <tls> requires a valid server certificate");

	// unix socket
	if (! str_empty(&config->path))
	{
		auto listen = server_listen_allocate(config);
		list_append(&self->listen, &listen->link);
		self->listen_count++;
		return;
	}

	if (str_empty(&config->host))
		error("server: listen[] <host> is not set"); 

	// listen '*'
	char* host = str_of(&config->host);
	if (str_size(&config->host) == 1 && *str_of(&config->host) == '*')
		host = NULL;

	// resolve
	resolve(global()->resolver, host, config->host_port, &config->host_addr);

	// foreach resolved address
	struct addrinfo* ai = config->host_addr;
	for (; ai; ai = ai->ai_next)
	{
		auto listen = server_listen_allocate(config);
		listen->addr = ai;
		list_append(&self->listen, &listen->link);
		self->listen_count++;
	}
}

static void
server_configure_unixsocket(Server* self)
{
	// add default unix socket to the repository
	auto config = server_config_allocate();
	str_dup_cstr(&config->path, "socket");
	config->path_mode = 0700;
	list_append(&self->config, &config->link);
	self->config_count++;
}

static void
server_configure(Server* self)
{
	// listen is not defined or null
	auto listen = &config()->listen;
	if (! opt_json_is_set(listen))
		return;
	auto pos = opt_json_of(listen);
	if (json_is_null(pos))
		return;
	if (! json_is_array(pos))
		error("server: <listen> option expected to be array or null"); 

	json_read_array(&pos);
	while (! json_read_array_end(&pos))
	{
		auto config = server_config_read(&pos);
		list_append(&self->config, &config->link);
		self->config_count++;
	}
}

static void
server_configure_tls(Server* self)
{
	// configure and create server tls context
	auto remote = &self->tls_remote;

	// tls_capath
	if (opt_string_is_set(&config()->tls_capath))
		remote_set_path(remote, REMOTE_PATH_CA, state_directory(),
		                &config()->tls_capath.string);

	// tls_ca
	if (opt_string_is_set(&config()->tls_ca))
		remote_set_path(remote, REMOTE_FILE_CA, state_directory(),
		                &config()->tls_ca.string);

	// tls_cert
	if (opt_string_is_set(&config()->tls_cert))
		remote_set_path(remote, REMOTE_FILE_CERT, state_directory(),
		                &config()->tls_cert.string);

	// tls_key
	if (opt_string_is_set(&config()->tls_key))
		remote_set_path(remote, REMOTE_FILE_KEY, state_directory(),
		                &config()->tls_key.string);

	// create tls context
	if (opt_string_is_set(&config()->tls_cert))
		tls_context_create(&self->tls, false, remote);
}

void
server_init(Server* self)
{
	self->on_connect     = NULL;
	self->on_connect_arg = NULL;
	self->listen_count   = 0;
	self->config_count   = 0;
	tls_context_init(&self->tls);
	remote_init(&self->tls_remote);
	list_init(&self->listen);
	list_init(&self->config);
}

void
server_free(Server* self)
{
	list_foreach_safe(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		server_listen_free(listen);
	}
	list_foreach_safe(&self->config)
	{
		auto config = list_at(ServerConfig, link);
		server_config_free(config);
	}
	tls_context_free(&self->tls);
	remote_free(&self->tls_remote);
}

void
server_start(Server*     self,
             ServerEvent on_connect,
             void*       on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;

	// read certificates
	server_configure_tls(self);

	// read listen configuration
	server_configure(self);

	// listen for unixsocket in the repository
	server_configure_unixsocket(self);

	if (! self->config_count)
		error("server: <listen> is not defined");

	// prepare listen objects
	list_foreach(&self->config)
	{
		auto config = list_at(ServerConfig, link);
		server_add(self, config);
	}

	// listen for incoming connections
	info("");
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		server_listen(listen);
	}

	// accept clients
	server_accept(self);
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
