
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
	buf_emplace(filler, 100);
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
		buf_format(&buf, "INSERT INTO bench_accounts VALUES ");
		for (auto i = 0ul; i < batch; i++)
		{
			buf_format(&buf, "{s}({d}, {d}, 0, \"{buf}\")",
			           i > 0 ? "," : "",
			           seq, seq / tpcb_accounts,
			           filler);
			seq++;
		}
		Str str;
		buf_str(&buf, &str);
		client_execute(client, &str, NULL);

		(*self->writes) += batch;
		(*self->transactions)++;
	}
}

static void
loader_main(void* arg)
{
	auto self   = (Loader*)arg;
	auto client = client_create();
	defer(client_free, client);
	client_set_endpoint(client, &self->bench->main->endpoint);

	error_catch
	(
		client_connect(client);
		loader_client_main(self, client);
	);

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
		info("{u64} transactions/sec, {.2f} millions writes/sec {u64}%",
		     tx - prev_tx,
		     (float)(wr - prev_wr) / 1000000.0,
		     (wr * 100ul) / accounts);
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
	buf_format(buf, "execute bench_tpcb({d}, {d}, {d}, {d});", bid, tid, aid, delta);
	Str cmd;
	buf_str(buf, &cmd);
	client_execute(client, &cmd, NULL);
}

static void
bench_tpcb_create(Bench* self, Client* client)
{
	info("preparing tables.");

	char* ddl[] =
	{
		"create table bench_branches (bid int primary key using hash, bbalance int, filler text)",
		"create table bench_tellers (tid int primary key using hash, bid int, tbalance int, filler text)",
		"create table bench_accounts (aid int primary key using hash, bid int, abalance int, filler text)",
		"create table bench_history (tid int, bid int, aid int, delta int, time timestamp, seq serial primary key, filler text)",
		 NULL
	};

	Str str;
	str_init(&str);
	for (auto i = 0; ddl[i]; i++)
	{
		str_set_cstr(&str, ddl[i]);
		client_execute(client, &str, NULL);
	}

	// create benchmark function
	char func[] =
	"create function bench_tpcb(bid int, tid int, aid int, delta int)"
	"begin"
	"	update bench_accounts a"
	"	   set abalance = a.abalance + delta"
	"	 where a.aid = aid;"
	""
	"	update bench_tellers t"
	"	   set tbalance = t.tbalance + delta"
	"	 where t.tid = tid;"
	""
	"	insert into bench_history(tid, bid, aid, delta, time, filler)"
	"	values(tid, bid, aid, delta, current_timestamp, '                                                  ');"
	"end;";

	char func_batch[] =
	"create function bench_tpcb_batch(batch int, scale int)  "
	"begin"
	"	declare n_branches int := 1      * scale;"
	"	declare n_tellers  int := 10     * scale;"
	" 	declare n_accounts int := 100000 * scale;"
	"	while batch > 0 do"
	"		select bench_tpcb(random() \% n_branches,"
	"		                  random() \% n_tellers,"
	"		                  random() \% n_accounts,"
	"		                  -5000 + (random() % 10000));"
	"		batch := batch - 1;"
	"	end;"
	"end;";

	str_set_cstr(&str, func);
	client_execute(client, &str, NULL);

	str_set_cstr(&str, func_batch);
	client_execute(client, &str, NULL);

	info("preparing data.");

	auto filler = buf_create();
	defer_buf(filler);
	buf_emplace(filler, 100);
	memset(filler->start, ' ', 100);

	auto batch = (int)opt_int_of(&self->batch);
	auto scale = (int)opt_int_of(&self->scale);
	auto buf = buf_create();
	defer_buf(buf);

	for (auto i = 0; i < tpcb_branches * scale; i++)
	{
		buf_reset(buf);
		buf_format(buf, "INSERT INTO bench_branches VALUES ({d}, 0, \"{buf}\")",
		           i, filler);
		buf_str(buf, &str);
		client_execute(client, &str, NULL);
	}

	for (auto i = 0; i < tpcb_tellers * scale; i++)
	{
		buf_reset(buf);
		buf_format(buf, "INSERT INTO bench_tellers VALUES ({d}, {d}, 0, \"{buf}\")",
		           i, i / tpcb_tellers,
		           filler);
		buf_str(buf, &str);
		client_execute(client, &str, NULL);
	}

	if (scale == 1)
	{
		for (auto i = 0; i < tpcb_accounts * scale; i++)
		{
			buf_reset(buf);
			buf_format(buf, "INSERT INTO bench_accounts VALUES ({d}, {d}, 0, \"{buf}\")",
			           i, i / tpcb_accounts,
			           filler);
			buf_str(buf, &str);
			client_execute(client, &str, NULL);
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
	auto bench = self->bench;
	int batch = opt_int_of(&bench->batch);
	int scale = opt_int_of(&bench->scale);
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_format(&buf, "execute bench_tpcb_batch({d}, {d});", batch, scale);
	Str cmd;
	buf_str(&buf, &cmd);
	while (! self->shutdown)
	{
		client_execute(client, &cmd, NULL);
		atomic_u64_add(&bench->transactions, batch);
		atomic_u64_add(&bench->writes, 3 * batch);
	}

#if 0
	int  scale    = opt_int_of(&bench->scale);
	auto accounts = tpcb_accounts * scale;
	auto branches = tpcb_branches * scale;
	auto tellers  = tpcb_tellers  * scale;

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_reserve(&buf, 512);

	Str cmd;
	buf_str(&buf, &cmd);
	while (! self->shutdown)
	{
		uint64_t random = random_generate(&runtime()->random);
		uint32_t a = random;
		uint32_t b = random >> 32;
		uint32_t c = a ^ b;
		int aid    = a % accounts;
		int bid    = b % branches;
		int tid    = c % tellers;
		int delta  = -5000 + (c % 10000); // -5000 to 5000

		buf_reset(&buf);
		buf_format(&buf, "execute __bench.tpcb({d}, {d}, {d}, {d});", bid, tid, aid, delta);
		bench_client_execute(client, &cmd);

		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, 3);
	}
#endif
}

BenchIf bench_tpcb =
{
	.create = bench_tpcb_create,
	.main   = bench_tpcb_main
};
