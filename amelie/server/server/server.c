
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
	ServerConfig* config = listen->config;
	Server*       self = listen->arg;

	auto               is_unixsocket = !listen->addr;
	char               addr_name[PATH_MAX];
	struct sockaddr_un addr_un;
	struct sockaddr*   addr;

	Exception e;
	if (enter(&e))
	{
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
				         config_directory(),
				         str_size(path), str_of(path));
			snprintf(addr_un.sun_path, sizeof(addr_un.sun_path) - 1, "%s", addr_name);
			// todo: remove old file, if exists

			addr = (struct sockaddr*)&addr_un;
		} else
		{
			addr = listen->addr->ai_addr;
			socket_getaddrname(addr, addr_name, sizeof(addr_name), true, true);
		}
		coroutine_set_name(am_self(), "listen %s", addr_name);

		// bind
		listen_start(&listen->listen, 4096, addr);

		// change unix socket mode
		if (is_unixsocket)
		{
			auto rc = chmod(addr_name, config->path_mode);
			if (rc == -1)
				error_system();
		}

		info("");

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
				if (config->tls)
					tcp_set_tls(&client->tcp, &self->tls);
				tcp_set_fd(&client->tcp, fd, addr->sa_family);
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

	// remove unix socket after use
	if (is_unixsocket)
		vfs_unlink(addr_name);

	listen_stop(&listen->listen);
}

static void
server_listen_add(Server* self, ServerConfig* config)
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
server_configure_unixsocket(Server* self)
{
	// add default unix socket to the repository
	auto config = server_config_allocate();
	str_strdup(&config->path, "amelie.sock");
	config->path_mode = 0700;
	list_append(&self->config, &config->link);
	self->config_count++;
}

static void
server_configure(Server* self)
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

static void
server_configure_tls(Server* self)
{
	// configure and create server tls context
	auto remote = &self->tls_remote;
	if (! fs_exists("%s/server.crt", config_directory_certs()))
		return;

	info("server: found TLS certificate");

	Str name;
	str_set_cstr(&name, "ca");
	remote_set_path(remote, REMOTE_PATH_CA, config_directory_certs(), &name);

	str_set_cstr(&name, "server.crt");
	remote_set_path(remote, REMOTE_FILE_CERT, config_directory_certs(), &name);

	str_set_cstr(&name, "server.key");
	remote_set_path(remote, REMOTE_FILE_KEY, config_directory_certs(), &name);

	// create tls context
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
	list_foreach_safe(&self->listen) {
		auto listen = list_at(ServerListen, link);
		server_listen_free(listen);
	}
	list_foreach_safe(&self->config) {
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

	// default listen for unixsocket in the repository
	server_configure_unixsocket(self);

	// read listen configuration
	server_configure(self);

	// listen for incoming clients
	server_listen(self);
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
