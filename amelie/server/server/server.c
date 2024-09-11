
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
server_add(Server* self, ServerConfig* config)
{
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
	resolve(host, config->host_port, &config->host_addr);

	// configure and create server tls context
	if (config->tls)
	{
		// validate server tls options
		auto remote = &config->remote;
		auto tls_cert = remote_get(remote, REMOTE_FILE_CERT);
		if (str_empty(tls_cert))
			error("server: <tls_cert> is not defined");
		auto tls_key  = remote_get(remote, REMOTE_FILE_KEY);
		if (str_empty(tls_key))
			error("server: <tls_key> is not defined");

		// create tls context
		tls_context_create(&config->tls_context, false, remote);
	}

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
server_listen(ServerListen* self)
{
	ServerConfig* config = self->config;
	char addr[PATH_MAX];

	// set listen address
	auto is_unixsocket = !self->addr;
	if (is_unixsocket)
	{
		auto path = &config->path;
		self->sa_un.sun_family = AF_UNIX;
		if (*str_of(path) == '/')
			snprintf(addr, sizeof(addr), "%.*s", str_size(path), str_of(path));
		else
			snprintf(addr, sizeof(addr), "%s/%.*s", config_directory(),
			         str_size(path), str_of(path));
		snprintf(self->sa_un.sun_path, sizeof(self->sa_un.sun_path) - 1, "%s", addr);
		self->sa = (struct sockaddr*)&self->sa_un;
	} else
	{
		self->sa = self->addr->ai_addr;
		socket_getaddrname(self->sa, addr, sizeof(addr), true, true);
	}
	str_strdup(&self->addr_name, addr);

	// bind
	listen_start(&self->listen, 4096, self->sa);

	// change unix socket mode
	if (is_unixsocket)
	{
		auto rc = chmod(addr, config->path_mode);
		if (rc == -1)
			error_system();
	}

	info("listen: %s", addr);
}

static void
server_accept(Server* self, ServerListen* listen)
{
	auto    config = listen->config;
	Client* client = NULL;
	int     fd     = -1;

	Exception e;
	if (enter(&e))
	{
		// accept
		fd = listen_accept(&listen->listen);

		// create new client
		client = client_create();
		client->arg = self;
		if (config->tls)
			tcp_set_tls(&client->tcp, &config->tls_context);
		tcp_set_fd(&client->tcp, fd, listen->sa->sa_family);
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

static void
server_main(Server* self)
{
	for (;;)
	{
		List pending;
		list_init(&pending);
		auto rc = poller_step(&am_self->poller, &pending, NULL, -1);
		if (rc == -1)
			error_system();
		if (rc == 0)
			continue;
		list_foreach(&pending)
		{
			auto fd = list_at(Fd, link);
			auto listen = container_of(fd, ServerListen, listen.fd);
			server_accept(self, listen);
		}
	}
}

void
server_start(Server*     self,
             ServerEvent on_connect,
             void*       on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;

	// read configuration
	server_configure(self);

	if (! self->config_count)
		error("server: <listen> is not defined");

	// prepare listen objects according to the config
	list_foreach(&self->config)
	{
		auto config = list_at(ServerConfig, link);
		server_add(self, config);
	}

	// start listen for incoming clients
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);
		server_listen(listen);
	}

	// process incoming connection
	server_main(self);
}

void
server_stop(Server* self)
{
	list_foreach(&self->listen)
	{
		auto listen = list_at(ServerListen, link);

		// remove unix socket after use
		auto is_unixsocket = !listen->addr;
		if (is_unixsocket)
			vfs_unlink(str_of(&listen->addr_name));

		listen_stop(&listen->listen);
	}
}
