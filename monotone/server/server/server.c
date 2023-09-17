
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>

hot static void
relay_main(void* arg)
{
	Client* client = arg;
	coroutine_set_name(mn_self(), "%s", "relay");

	Exception e;
	if (try(&e))
	{
		// attach client channel
		channel_attach(&client->core);

		// create remote connection
		auto conn = connection_create(ACCESS_CLIENT);
		guard(conn_guard, connection_free, conn);
		connection_set_uri(conn, false, &client->uri);
		connection_connect(conn);

		// notify api
		client_on_connect(client);

		// retransmit data
		relay(conn, &client->core, &client->src);
	}
	if (catch(&e))
	{}

	// detach ipc
	channel_detach(&client->core);
	client_on_disconnect(client);
}

hot static void
connection_main(void* arg)
{
	Connection* conn = arg;
	Server* self = conn->arg;

	connection_mgr_add(&self->connection_mgr, conn);

	// client, backup, replica
	Client client;
	client_init(&client);

	Exception e;
	if (try(&e))
	{
		// authenticate
		connection_attach(conn);
		connection_accept(conn, &self->user_mgr);

		// prepare client
		channel_attach(&client.src);
		client_set_access(&client, conn->access);
		client_set_mode(&client, conn->mode);
		client_set_auth(&client, &conn->auth);

		// connect client to the core
		auto buf = client_connect(&client, mn_task->buf_cache);
		if (unlikely(buf == NULL))
			error_system();
		channel_write(global()->control->core, buf);

		// todo: wait for connect?
		client_set_connected(&client, true);

		/* retransmit data */
		bool disconnect;
		disconnect = relay(conn, &client.src, &client.core);
		if (disconnect)
		{
			// on core disconnect event (not connection)
			client_set_connected(&client, false);
		}
	}
	if (catch(&e))
	{}

	connection_mgr_del(&self->connection_mgr, conn);
	connection_free(conn);

	// disconnect if still connected to the core
	client_close(&client, mn_task->buf_cache);
	channel_detach(&client.src);
	client_free(&client, mn_task->buf_cache);
}

static void
server_listen_main(void* arg)
{
	ServerListen* listen = arg;
	Server*       self = listen->arg;

	Exception e;
	if (try(&e))
	{
		// set listen address
		char addr_name[128];
		socket_getaddrname(listen->addr->ai_addr, addr_name, sizeof(addr_name), true, true);
		coroutine_set_name(mn_self(), "listen %s", addr_name);

		// bind
		listen_start(&listen->listen, 4096, listen->addr->ai_addr);
		log("start");

		// process incoming connection
		for (;;)
		{
			// cancellation point
			int fd = listen_accept(&listen->listen);

			Connection* conn = NULL;
			Exception e;
			if (try(&e))
			{
				// create new connection
				conn = connection_create(ACCESS_UNDEF);
				conn->arg = self;
				if (listen->tls_context)
					tcp_set_tls(&conn->tcp, listen->tls_context);
				tcp_set_fd(&conn->tcp, fd);
				fd = -1;

				// create coroutine per connection
				coroutine_create(connection_main, conn);
			}

			if (catch(&e))
			{
				if (conn)
					connection_free(conn);
				if (fd != -1)
					socket_close(fd);
			}
		}
	}
	if (catch(&e))
	{}

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

		// create tsl context
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
server_rpc(Rpc* rpc, void* arg)
{
	Server* self = arg;

	switch (rpc->id) {
#if 0
	case RPC_USER_CREATE:
	{
		UserConfig* config = rpc_arg_ptr(rpc, 0);
		bool if_not_exists = rpc_arg(rpc, 1);
		user_mgr_create(&self->user_mgr, config, if_not_exists);
		break;
	}
	case RPC_USER_DROP:
	{
        char *name = rpc_arg_ptr(rpc, 0);
        int   name_size = rpc_arg(rpc, 1);
        bool  if_exists = rpc_arg(rpc, 2);
        user_mgr_drop(&self->user_mgr, name, name_size, if_exists);
		break;
	}
	case RPC_USER_ALTER:
	{
        user_config *config = rpc_arg_ptr(rpc, 0);
        user_mgr_alter(&self->user_mgr, config);
		break;
	}
	case RPC_USER_SHOW:
	{
        buf **buf = rpc_arg_ptr(rpc, 0);
        *buf = user_mgr_list(&self->user_mgr);
		break;
	}
#endif 
	case RPC_STOP:
	{
		// stop listen
		list_foreach(&self->listen)
		{
			auto listen = list_at(ServerListen, link);
			coroutine_kill(listen->worker);
		}

		// disconnect clients
		connection_mgr_shutdown(&self->connection_mgr);
		break;
	}
	default:
		break;
	}
}

static void
server_main(void* arg)
{
	Server* self = arg;

	// listen for incoming clients
	server_listen(self);

	bool stop = false;
	while (! stop)
	{
		auto buf = channel_read(&mn_task->channel, -1);
		auto msg = msg_of(buf);

		// relay client connection
		if (msg->id == MSG_CLIENT)
		{
			Client* client = *(void**)msg->data;
			buf_free(buf);
			client->arg = self;
			coroutine_create(relay_main, client);
			continue;
		}

		// server command
		stop = msg->id == RPC_STOP;
		rpc_execute(buf, server_rpc, self);
		buf_free(buf);
	}
}

void
server_init(Server* self)
{
	self->listen_count = 0;
	self->config_count = 0;
	list_init(&self->listen);
	list_init(&self->config);
	user_mgr_init(&self->user_mgr);
	task_init(&self->task, mn_task->buf_cache);
	connection_mgr_init(&self->connection_mgr);
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
	user_mgr_free(&self->user_mgr);
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

	int count;
	data_read_array(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		auto config = server_config_read(&pos);
		list_append(&self->config, &config->link);
		self->config_count++;
	}
}

void
server_start(Server* self, bool listen)
{
	if (listen)
	{
		// restore users
		user_mgr_open(&self->user_mgr);

		// configure server
		server_configure(self);
	}

	// create task
	task_create(&self->task, "server", server_main, self);
}

void
server_stop(Server* self)
{
	// send stop request
	if (task_active(&self->task))
	{
		rpc(&self->task.channel, RPC_STOP, 0);
		task_wait(&self->task);
		task_free(&self->task);
		task_init(&self->task, mn_task->buf_cache);
	}
}
