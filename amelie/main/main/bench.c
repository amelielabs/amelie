
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

hot static inline void
bench_execute(Client* client, Remote* remote, Str* cmd)
{
	auto request = &client->request;
	auto reply   = &client->reply;
	auto token   = remote_get(remote, REMOTE_TOKEN);

	// request
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Length", "%d", str_size(cmd));
	http_write(request, "Content-Type", "text/plain");
	http_write_end(request);
	tcp_write_pair_str(&client->tcp, &request->raw, cmd);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);
}

hot static void
bench_insert(BenchWorker* self, Client* client)
{
	auto bench = self->bench;

	Str cmd;
	str_init(&cmd);
	str_set_cstr(&cmd, "insert into __test generate 500");

	while (! self->shutdown)
	{
		bench_execute(client, bench->remote, &cmd);
		atomic_u64_add(&bench->transactions, 1);
	}
}

static void
bench_connection(void* arg)
{
	BenchWorker* self = arg;
	Client* client = NULL;

	Exception e;
	if (enter(&e))
	{
		// create client and connect
		client = client_create();
		client_set_remote(client, self->bench->remote);
		client_connect(client);

		// process
		bench_insert(self, client);
	}

	if (leave(&e))
	{ }

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}

static void
bench_worker_main(void* arg)
{
	BenchWorker* self = arg;

	// start connections
	int connections = self->connections;
	while (connections-- > 0)
		coroutine_create(bench_connection, self);

	// wait for stop
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);
		if (msg->id == RPC_STOP)
			break;
	}

	// wait for completion
	self->shutdown = true;
	while (am_task->coroutine_mgr.count > 1)
		coroutine_yield();
}

static BenchWorker*
bench_worker_allocate(Bench* bench, int connections)
{
	auto self = (BenchWorker*)am_malloc(sizeof(BenchWorker));
	self->connections = connections;
	self->shutdown    = false;
	self->bench       = bench;
	task_init(&self->task);
	list_init(&self->link);
	return self;
}

static void
bench_worker_free(BenchWorker* self)
{
	task_free(&self->task);
	am_free(self);
}

static void
bench_worker_start(BenchWorker* self)
{
	task_create(&self->task, "bench", bench_worker_main, self);
}

static void
bench_worker_stop_notify(BenchWorker* self)
{
	auto buf = msg_create_nothrow(&self->task.buf_cache, RPC_STOP, 0);
	if (! buf)
		abort();
	channel_write(&self->task.channel, buf);
}

static void
bench_worker_stop(BenchWorker* self)
{
	task_wait(&self->task);
}

void
bench_init(Bench* self)
{
	memset(self, 0, sizeof(*self));

	self->opt_threads     = 4;
	self->opt_connections = 12;
	self->opt_duration    = 10;
	self->opt_batch       = 500;

	list_init(&self->list);
}

void
bench_free(Bench* self)
{
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_free(worker);
	}
}

void
bench_set_threads(Bench* self, int value)
{
	self->opt_threads = value;
}

void
bench_set_connections(Bench* self, int value)
{
	self->opt_connections = value;
}

void
bench_set_duration(Bench* self, int value)
{
	self->opt_duration = value;
}

void
bench_set_remote(Bench* self, Remote* remote)
{
	self->remote = remote;
}

static void
bench_service_init(Bench* self, Client* client)
{
	Str str;
	str_set_cstr(&str, "create table __test (id int primary key serial)");
	bench_execute(client, self->remote, &str);
}

static void
bench_service_cleanup(Bench* self, Client* client)
{
	Str str;
	str_set_cstr(&str, "drop table __test");
	bench_execute(client, self->remote, &str);
}

static void
bench_service(Bench* self, bool init)
{
	Client* client = NULL;

	Exception e;
	if (enter(&e))
	{
		// create client and connect
		client = client_create();
		client_set_remote(client, self->remote);
		client_connect(client);

		// process
		if (init)
			bench_service_init(self, client);
		else
			bench_service_cleanup(self, client);
	}

	if (leave(&e))
	{ }

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}

void
bench_run(Bench* self)
{
	auto workers = self->opt_threads;
	auto connections = self->opt_connections / workers;
	while (workers-- > 0)
	{
		auto worker = bench_worker_allocate(self, connections);
		list_append(&self->list, &worker->link);
	}

	info("amelie benchmark.");
	info("");
	info("duration:     %d sec", self->opt_duration);
	info("threads:      %d", self->opt_threads);
	info("connections:  %d (%d per thread)", self->opt_connections,
	     connections);
	info("duration:     %d sec", self->opt_duration);
	info("");

	// prepare tables
	bench_service(self, true);

	// begin
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_start(worker);
	}

	uint64_t prev = 0;
	for (auto duration = self->opt_duration; duration > 0; duration--)
	{
		coroutine_sleep(1000);
		auto n = atomic_u64_of(&self->transactions) * self->opt_batch;
		info("rps %" PRIu64 " sec", n - prev);
		prev = n;
	}

	// stop
	list_foreach(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_stop_notify(worker);
	}
	list_foreach_safe(&self->list)
	{
		auto worker = list_at(BenchWorker, link);
		bench_worker_stop(worker);
	}

	// cleanup
	bench_service(self, false);

	// report
	info("");
	info("transactions: %" PRIu64, self->transactions);
	info("writes:       %" PRIu64, self->transactions * self->opt_batch);
}
