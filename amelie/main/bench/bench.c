
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_bench.h>

void
bench_init(Bench* self, Main* main)
{
	memset(self, 0, sizeof(*self));
	self->main = main;
	opts_init(&self->opts);
	OptsDef defs[] =
	{
		{ "type",      OPT_STRING, OPT_C,       &self->opt_type,     "tpcb", 0     },
		{ "users",     OPT_INT,    OPT_C|OPT_Z, &self->opt_users,     NULL,  1     },
		{ "clients",   OPT_INT,    OPT_C|OPT_Z, &self->opt_clients,   NULL,  32    },
		{ "duration",  OPT_INT,    OPT_C|OPT_Z, &self->opt_duration,  NULL,  10    },
		{ "scale",     OPT_INT,    OPT_C|OPT_Z, &self->opt_scale,     NULL,  1     },
		{ "batch",     OPT_INT,    OPT_C|OPT_Z, &self->opt_batch,     NULL,  64    },
		{ "histogram", OPT_BOOL,   OPT_C,       &self->opt_histogram, NULL,  false },
		{  NULL,       0,          0,            NULL,                NULL,  0     }
	};
	opts_define(&self->opts, defs);
}

void
bench_free(Bench* self)
{
	if (self->clients)
	{
		for (auto i = 0; i < self->clients_count; i++)
		{
			auto client = &self->clients[i];
			if (client->client)
				client_free(client->client);
			endpoint_free(&client->endpoint);
		}
		am_free(self->clients);
	}
	opts_free(&self->opts);
}

static void
bench_deploy_client(BenchClient* self, int user_id, bool deploy)
{
	auto endpoint_main = &self->bench->main->endpoint;
	auto endpoint = &self->endpoint;

	// set host/path
	auto path = &endpoint_main->path;
	auto host = &endpoint_main->host;
	auto port = &endpoint_main->port;
	if (! opt_string_empty(path))
		opt_string_set(&endpoint->path, &path->string);
	if (! opt_string_empty(host))
		opt_string_set(&endpoint->host, &host->string);
	opt_int_set(&endpoint->port, port->integer);

	// set user
	char name[16];
	format(name, sizeof(name), "bench_{d}", user_id);
	Str str;
	str_set_cstr(&str, name);
	opt_string_set(&endpoint->user, &str);
	endpoint_auth(endpoint);

	// connnect
	self->client = client_allocate();
	client_set_endpoint(self->client, endpoint);
	if (opt_int_of(&self->bench->opt_histogram))
		client_set_histogram(self->client, &self->bench->histogram);
	client_connect(self->client);

	// create user relations
	if (deploy)
		self->bench->iface->deploy(self);
}

static void
bench_create(Bench* self)
{
	// create users (under superuser)
	auto users  = (int)self->opt_users.integer;
	auto client = client_allocate();
	defer(client_free, client);
	client_set_endpoint(client, &self->main->endpoint);
	client_connect(client);
	for (auto i = 0; i < users; i++)
	{
		char name[16];
		format(name, sizeof(name), "bench_{d}", i);
		Str str;
		str_set_cstr(&str, name);

		char cmd[256];
		format(cmd, sizeof(cmd), "drop user if exists {str} cascade", &str);
		client_run(client, cmd);

		format(cmd, sizeof(cmd), "create user {str}", &str);
		client_run(client, cmd);
	}
}

static void
bench_deploy(Bench* self)
{
	// prepare clients
	auto users   = (int)self->opt_users.integer;
	auto clients = (int)self->opt_clients.integer;
	auto size    = sizeof(BenchClient) * users * clients;

	// create users
	bench_create(self);

	self->clients_count = users * clients;
	self->clients = am_malloc(size);
	for (auto i = 0; i < self->clients_count; i++)
	{
		auto at = &self->clients[i];
		at->client = NULL;
		at->bench  = self;
		endpoint_init(&at->endpoint);
	}

	// connect clients
	auto seq = 0;
	for (auto i = 0; i < users; i++)
	{
		auto at = &self->clients[seq];
		bench_deploy_client(at, i, true);
		seq++;
		for (auto j = 1; j < clients; j++)
		{
			at = &self->clients[seq];
			bench_deploy_client(at, i, false);
			seq++;
		}
	}
}

static void
bench_client(void* arg)
{
	BenchClient* self = arg;
	error_catch (
		self->bench->iface->main(self);
	);
}

static void
bench_main(Bench* self)
{
	// start
	for (auto i = 0; i < self->clients_count; i++)
	{
		auto client = &self->clients[i];
		coroutine_create(bench_client, client);
	}

	// report
	auto duration = opt_int_of(&self->opt_duration);
	uint64_t prev_tx = 0;
	uint64_t prev_wr = 0;
	for (; duration > 0; duration--)
	{
		coroutine_sleep(1000);
		auto tx = self->transactions;
		auto wr = self->writes;
		if (wr > 0)
			info("{u64} transactions/sec, {.2f} millions writes/sec ({u64})",
			     tx - prev_tx,
			     (float)(wr - prev_wr) / 1000000.0,
			     wr - prev_wr);
		else
			info("{u64} transactions/sec", tx - prev_tx);
		prev_tx = tx;
		prev_wr = wr;
	}

	// stop and wait for completion
	self->shutdown = true;
	while (am_task->coroutines.count > 1)
		coroutine_yield();

	// report
	info("");
	info("transactions: {u64}", self->transactions);
	info("writes:       {.2f} millions writes ({u64})",
	     self->writes / 1000000.0,
	     self->writes);
	info("");
	auto histogram = opt_int_of(&self->opt_histogram);
	if (histogram)
		histogram_print(&self->histogram);
}

void
bench_run(Bench* self)
{
	info("");
	info("amelie benchmark.");
	info("");
	info("type:     {str}",     opt_string_of(&self->opt_type));
	info("duration: {u64} sec", opt_int_of(&self->opt_duration));
	info("users:    {u64} ",    opt_int_of(&self->opt_users));
	info("clients:  {u64} ",    opt_int_of(&self->opt_clients));
	info("batch:    {u64}",     opt_int_of(&self->opt_batch));
	info("");

	// set benchmark
	auto type = opt_string_of(&self->opt_type);
	if (str_is_cstr(type, "insert"))
		self->iface = &bench_insert;
	else
	if (str_is_cstr(type, "upsert"))
		self->iface = &bench_upsert;
	else
	if (str_is_cstr(type, "tpcb"))
		self->iface = &bench_tpcb;
	else
	if (str_is_cstr(type, "decre"))
		self->iface = &bench_decre;
	else
	if (str_is_cstr(type, "vector"))
		self->iface = &bench_vector;
	else
		error("unknown benchmark type '{str}'", type);

	// create clients and deploy
	bench_deploy(self);
	bench_main(self);
}
