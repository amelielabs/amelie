
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

#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_bench.h>

static const int tpcb_branches = 1;
static const int tpcb_tellers  = 10;
static const int tpcb_accounts = 100000;

typedef struct Loader Loader;

struct Loader
{
	int       from;
	int       to;
	int       batch;
	uint64_t* transactions;
	uint64_t* writes;
	int*      complete;
	Bench*    bench;
};

static inline void
loader_init(Loader*   self, Bench* bench,
            int       from,
            int       to,
            int       batch,
            uint64_t* transactions,
            uint64_t* writes,
            int*      complete)
{
	self->from         = from;
	self->to           = to;
	self->batch        = batch;
	self->transactions = transactions;
	self->writes       = writes;
	self->complete     = complete;
	self->bench        = bench;
}

static void
loader_client_main(Loader* self, Client* client)
{
	auto filler = buf_create();
	defer_buf(filler);
	buf_claim(filler, 100);
	memset(filler->start, ' ', 100);

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	for (auto seq = self->from; seq < self->to;)
	{
		auto batch = 0ul;
		if (self->to - seq >= self->batch)
			batch = self->batch;
		else
			batch = self->to - seq;

		buf_reset(&buf);
		buf_printf(&buf, "INSERT INTO __bench.accounts VALUES ");
		for (auto i = 0ul; i < batch; i++)
		{
			buf_printf(&buf, "%s(%d, %d, 0, \"%.*s\")",
			           i > 0 ? "," : "",
			           seq, seq / tpcb_accounts,
			           buf_size(filler), filler->start);
			seq++;
		}
		Str str;
		buf_str(&buf, &str);
		client_execute(client, &str);

		(*self->writes) += batch;
		(*self->transactions)++;
	}
}

static void
loader_main(void* arg)
{
	auto self = (Loader*)arg;

	Client* client = NULL;
	error_catch
	(
		// create client and connect
		client = client_create();
		client_set_remote(client, self->bench->remote);
		client_connect(client);

		// process
		loader_client_main(self, client);
	);

	if (client)
	{
		client_close(client);
		client_free(client);
	}

	(*self->complete)++;
}

static void
bench_tpcb_load(Bench* self, int scale, int batch, int clients)
{
	int     loaders_complete = 0;
	int     loaders_count = clients;
	Loader* loaders = am_malloc(sizeof(Loader) * loaders_count);
	defer(am_free, loaders);

	// prepare client connections
	uint64_t transactions = 0;
	uint64_t writes       = 0;
	auto     accounts     = tpcb_accounts * scale;
	auto     step         = accounts / loaders_count;
	auto     from         = 0ul;
	for (auto i = 0; i < loaders_count; i++)
	{
		auto next = 0ul;
		if ((i + 1) < loaders_count)
			next = step;
		else
			next = accounts - from;
		auto loader = &loaders[i];
		loader_init(loader, self, from, from + next, batch,
		            &transactions,
		            &writes,
		            &loaders_complete);
		coroutine_create(loader_main, loader);
		from += next;
	}

	// wait for completion and report
	uint64_t prev_tx = 0;
	uint64_t prev_wr = 0;
	while (loaders_complete != loaders_count)
	{
		coroutine_sleep(1000);
		auto tx = transactions;
		auto wr = writes;
		info("%" PRIu64 " transactions/sec, %.2f millions writes/sec %" PRIu64 "%%",
		     tx - prev_tx,
		     (float)(wr - prev_wr) / 1000000.0,
		     (wr * 100ull) / accounts);
		prev_tx = tx;
		prev_wr = wr;
	}
}

hot static inline void
tpcb_execute(Client* client,
             int     bid,
             int     tid,
             int     aid, int delta)
{
	auto buf = buf_create();
	defer_buf(buf);

	buf_printf(buf, "UPDATE __bench.accounts SET abalance = abalance + %d WHERE aid = %d;",
	           delta, aid);
	buf_printf(buf, "SELECT abalance FROM __bench.accounts WHERE aid = %d;", aid);
	buf_printf(buf, "UPDATE __bench.tellers SET tbalance = tbalance + %d WHERE tid = %d;",
	           delta, tid);
	buf_printf(buf, "UPDATE __bench.branches SET bbalance = bbalance + %d WHERE bid = %d;",
	           delta, bid);
	buf_printf(buf, "INSERT INTO __bench.history (tid, bid, aid, delta, time, filler) "
	           "VALUES (%d, %d, %d, %d, current_timestamp, '                                                  ');",
	           tid, bid, aid, delta);

	Str cmd;
	buf_str(buf, &cmd);
	client_execute(client, &cmd);
}

static void
bench_tpcb_create(Bench* self, Client* client)
{
	info("preparing tables.");

	char* ddl[] =
	{
		"create table __bench.branches (bid int primary key, bbalance int, filler text) with (type = \"hash\")",
		"create table __bench.tellers (tid int primary key, bid int, tbalance int, filler text) with (type = \"hash\")",
		"create table __bench.accounts (aid int primary key, bid int, abalance int, filler text) with (type = \"hash\")",
		"create table __bench.history (tid int, bid int, aid int, delta int, time timestamp, seq serial primary key, filler text)",
		 NULL
	};

	Str str;
	str_init(&str);
	for (auto i = 0; ddl[i]; i++)
	{
		str_set_cstr(&str, ddl[i]);
		client_execute(client, &str);
	}

	if (opt_int_of(&self->unlogged))
	{
		char* ddl_unlogged[] =
		{
			"alter table __bench.branches set unlogged",
			"alter table __bench.tellers set unlogged",
			"alter table __bench.accounts set unlogged",
			"alter table __bench.history set unlogged",
		     NULL
		};
		for (auto i = 0; ddl_unlogged[i]; i++)
		{
			str_set_cstr(&str, ddl_unlogged[i]);
			client_execute(client, &str);
		}
	}

	info("preparing data.");

	auto filler = buf_create();
	defer_buf(filler);
	buf_claim(filler, 100);
	memset(filler->start, ' ', 100);

	auto batch = opt_int_of(&self->batch);
	auto scale = opt_int_of(&self->scale);
	auto buf = buf_create();
	defer_buf(buf);

	for (auto i = 0ul; i < tpcb_branches * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO __bench.branches VALUES (%d, 0, \"%.*s\")",
		           i,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	for (auto i = 0ul; i < tpcb_tellers * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO __bench.tellers VALUES (%d, %d, 0, \"%.*s\")",
		           i, i / tpcb_tellers,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	if (scale == 1)
	{
		for (auto i = 0ul; i < tpcb_accounts * scale; i++)
		{
			buf_reset(buf);
			buf_printf(buf, "INSERT INTO __bench.accounts VALUES (%d, %d, 0, \"%.*s\")",
			           i, i / tpcb_accounts,
			           buf_size(filler), filler->start);
			buf_str(buf, &str);
			client_execute(client, &str);
		}
	} else {
		auto clients = opt_int_of(&self->clients);
		bench_tpcb_load(self, scale, batch, clients);
	}

	// add 100 to account 1 by teller 1 at branch 1
	tpcb_execute(client, 1, 1, 1, 100);

	info("done.");
	info("");
}

hot static void
bench_tpcb_main(BenchWorker* self, Client* client)
{
	auto bench  = self->bench;

	int  scale    = opt_int_of(&bench->scale);
	auto accounts = tpcb_accounts * scale;
	auto branches = tpcb_branches * scale;
	auto tellers  = tpcb_tellers  * scale;

	while (! self->shutdown)
	{
		uint64_t random = random_generate(global()->random); 
		uint32_t a = *(uint32_t*)(&random);
		uint32_t b = *(uint32_t*)((uint8_t*)&random + sizeof(uint32_t));
		uint32_t c = a ^ b;
		int aid    = a % accounts;
		int bid    = b % branches;
		int tid    = c % tellers;
		int delta  = -5000 + (c % 10000); // -5000 to 5000

		tpcb_execute(client, bid, tid, aid, delta);

		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, 4);
	}
}

BenchIf bench_tpcb =
{
	.create = bench_tpcb_create,
	.main   = bench_tpcb_main
};
