
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

static const int tpcb_branches = 1;
static const int tpcb_tellers  = 10;
static const int tpcb_accounts = 100000;

hot static inline void
tpcb_send(Client* client,
          long    bid,
          long    tid,
          long    aid, long delta)
{
	auto buf = buf_create();
	guard_buf(buf);

	buf_printf(buf, "UPDATE __bench.accounts SET abalance = abalance + %d WHERE aid = %d;",
	           delta, aid);
	buf_printf(buf, "SELECT abalance FROM __bench.accounts WHERE aid = %d;", aid);
	buf_printf(buf, "UPDATE __bench.tellers SET tbalance = tbalance + %d WHERE tid = %d;",
	           delta, tid);
	buf_printf(buf, "UPDATE __bench.branches SET bbalance = bbalance + %d WHERE bid = %d;",
	           delta, bid);
	buf_printf(buf, "INSERT INTO __bench.history (tid, bid, aid, delta, time, filler) "
	           "VALUES (%d, %d, %d, %d, current_timestamp, \"                                                            \");",
	           tid, bid, aid, delta);

	Str cmd;
	buf_str(buf, &cmd);
	client_send(client, &cmd);
}

hot static void
bench_tpcb_send(Bench* self, Client* client)
{
	int  scale    = var_int_of(&self->scale);
	auto accounts = tpcb_accounts * scale;
	auto branches = tpcb_branches * scale;
	auto tellers  = tpcb_tellers  * scale;

	uint64_t random = random_generate(global()->random);
	uint32_t a = *(uint32_t*)(&random);
	uint32_t b = *(uint32_t*)((uint8_t*)&random + sizeof(uint32_t));
	uint32_t c = a ^ b;

	int aid    = a / accounts;
	int bid    = b / branches;
	int tid    = c / tellers;
	int delta  = -5000 + (c % 10000); // -5000 to 5000
	if (! aid)
		aid++;
	if (! bid)
		bid++;
	if (! tid)
		tid++;

	tpcb_send(client, bid, tid, aid, delta);
}

hot static void
bench_tpcb_recv(Bench* self, Client* client)
{
	client_recv(client);

	atomic_u64_add(&self->transactions, 1);
	atomic_u64_add(&self->writes, 4);
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
		"create table __bench.history (tid int, bid int, aid int, delta int, time timestamp, seq int primary key serial, filler text) with (type = \"hash\")",
		 NULL
	};

	Str str;
	str_init(&str);
	for (auto i = 0; ddl[i]; i++)
	{
		str_set_cstr(&str, ddl[i]);
		client_execute(client, &str);
	}

	info("preparing data.");

	auto filler = buf_create();
	guard_buf(filler);
	buf_claim(filler, 100);
	memset(filler->start, ' ', 100);

	auto buf = buf_create();
	guard_buf(buf);

	int scale = var_int_of(&self->scale);
	for (auto i = 0; i < tpcb_branches * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO __bench.branches VALUES (%d, 0, \"%.*s\")",
		           i,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	for (auto i = 0; i < tpcb_tellers * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO __bench.tellers VALUES (%d, %d, 0, \"%.*s\")",
		           i, i / tpcb_tellers,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	for (auto i = 0; i < tpcb_accounts * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO __bench.accounts VALUES (%d, %d, 0, \"%.*s\")",
		           i, i / tpcb_accounts,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	// add 100 to account 1 by teller 1 at branch 1
	tpcb_send(client, 1, 1, 1, 100);

	info("done.");
	info("");
}

BenchIf bench_tpcb =
{
	.send   = bench_tpcb_send,
	.recv   = bench_tpcb_recv,
	.create = bench_tpcb_create
};
