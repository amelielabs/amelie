
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>

Server*
server_allocate(ServerConfig* config,
                ServerEvent   on_connect,
                void*         on_connect_arg)
{
	auto self = (Server*)am_malloc(sizeof(Server));
	self->config         = config;
	self->worker         = -1;
	self->addr           = NULL;
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;
	server_config_ref(config);
	str_init(&self->addr_name);
	listen_init(&self->listen);
	list_init(&self->link);
	return self;
}

void
server_free(Server* self)
{
	listen_stop(&self->listen);
	str_free(&self->addr_name);
	server_config_unref(self->config);
	am_free(self);
}

void
server_listen(Server* self)
{
	// listen and bind
	auto               is_unixsocket = !self->addr;
	char               addr_name[PATH_MAX];
	struct sockaddr_un addr_un;
	struct sockaddr*   addr;

	// set listen address
	if (is_unixsocket)
	{
		memset(&addr_un, 0, sizeof(addr_un));
		addr_un.sun_family = AF_UNIX;
		format(addr_name, sizeof(addr_name), "{s}/socket",
		       state_directory());
		format(addr_un.sun_path, sizeof(addr_un.sun_path), "{s}",
		       addr_name);
		vfs_unlink(addr_name);
		addr = (struct sockaddr*)&addr_un;
	} else
	{
		addr = self->addr->ai_addr;
		socket_getaddrname(addr, addr_name, sizeof(addr_name), true, true);
	}

	// set address name
	str_dup_cstr(&self->addr_name, addr_name);

	info("listening on '{str}'", &self->addr_name);

	// bind
	listen_start(&self->listen, 4096, addr);

	// change unix socket mode
	if (is_unixsocket)
	{
		auto rc = chmod(addr_name, 0700);
		if (rc == -1)
			error_system();
	}
}

static inline void
server_accept(Server* self)
{
	ServerConfig* config = self->config;

	// cancellation point
	auto fd = listen_accept(&self->listen);

	Client* client = NULL;
	auto on_error = error_catch
	(
		// create new client
		client = client_create();
		client_set_trusted(client, config->trusted);
		if (config->tls)
			tcp_set_tls(&client->tcp, &config->tls_context);
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
server_main(void* arg)
{
	Server* self = arg;
	coroutine_set_name(am_self(), "server");
	coroutine_set_cancel_log(am_self(), false);

	// process incoming connections
	error_catch
	(
		for (;;)
			server_accept(self);
	);

	// remove unix socket after use
	if (! self->addr)
		vfs_unlink(str_of(&self->addr_name));

	listen_stop(&self->listen);
}

void
server_start(Server* self)
{
	self->worker = coroutine_create(server_main, self);
}

void
server_stop(Server* self)
{
	if (self->worker != -1)
	{
		coroutine_kill(self->worker);
		self->worker = -1;
	}
}
