
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

static void
server_mgr_create_unixsocket(ServerMgr* self)
{
	auto config = server_config_allocate();
	auto server = server_allocate(config, self->on_connect, self->on_connect_arg);
	list_append(&self->list, &server->link);
	self->list_count++;
}

static void
server_mgr_create(ServerMgr* self, ServerConfig* config)
{
	// ensure certificate has been loaded
	if (config->tls && !tls_context_created(&config->tls_context))
		error("server: 'tls' requires a valid server certificate");

	if (str_empty(&config->host))
		error("server: 'host' is not set"); 

	// listen '*'
	auto host = str_of(&config->host);
	if (str_size(&config->host) == 1 && *str_of(&config->host) == '*')
		host = NULL;

	// resolve
	resolve(host, config->port, &config->ai);

	// create server for each resolved address
	for (auto ai = config->ai; ai; ai = ai->ai_next)
	{
		auto server = server_allocate(config, self->on_connect, self->on_connect_arg);
		server->addr = ai;
		list_append(&self->list, &server->link);
		self->list_count++;
	}
}

void
server_mgr_open(ServerMgr* self)
{
	// read servers configurations
	auto pos = opt_json_of(&config()->listen);
	if (pos)
	{
		if (! data_is_array(pos))
			error("server: server config must start with []");

		unpack_array(&pos);
		while (! unpack_array_end(&pos))
		{
			auto config = server_config_read(&pos);
			errdefer(server_config_free, config);
			server_mgr_create(self, config);
		}
	}

	// add default unix socket to the repository
	server_mgr_create_unixsocket(self);
}

void
server_mgr_start(ServerMgr* self)
{

	// listen for incoming connections
	list_foreach(&self->list)
	{
		auto server = list_at(Server, link);
		server_listen(server);
	}

	// start accepting incoming clients
	list_foreach(&self->list)
	{
		auto server = list_at(Server, link);
		server_start(server);
	}
}

void
server_mgr_stop(ServerMgr* self)
{
	list_foreach(&self->list)
	{
		auto server = list_at(Server, link);
		server_stop(server);
	}
}

void
server_mgr_init(ServerMgr*  self,
                ServerEvent on_connect,
                void*       on_connect_arg)
{
	self->on_connect     = on_connect;
	self->on_connect_arg = on_connect_arg;
	self->list_count     = 0;
	list_init(&self->list);
}

void
server_mgr_free(ServerMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto server = list_at(Server, link);
		server_free(server);
	}
}
