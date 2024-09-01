
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
tpcb_execute(Client* client, Buf* filler,
             long    bid,
             long    tid,
             long    aid, long delta)
{
	auto buf = buf_create();
	guard_buf(buf);

	buf_printf(buf, "UPDATE accounts SET abalance = abalance + %d WHERE aid = %d;",
	           delta, aid);
	buf_printf(buf, "SELECT abalance FROM accounts WHERE aid = %d;", aid);
	buf_printf(buf, "UPDATE tellers SET tbalance = tbalance + %d WHERE tid = %d;",
	           delta, tid);
	buf_printf(buf, "UPDATE branches SET bbalance = bbalance + %d WHERE bid = %d;",
	           delta, bid);
	buf_printf(buf, "INSERT INTO history (tid, bid, aid, delta, time, filler) VALUES (%d, %d, %d, %d, current_timestamp, \"%.*s\");",
	           tid, bid, aid, delta, 50, filler->start);

	Str cmd;
	buf_str(buf, &cmd);
	client_execute(client, &cmd);
}

static void
bench_tpcb_init(Bench* self, Client* client)
{
	info("preparing tables.");

	char* ddl[] =
	{
		"create table branches (bid int primary key, bbalance int, filler text) with (type = \"hash\")",
		"create table tellers (tid int primary key, bid int, tbalance int, filler text) with (type = \"hash\")",
		"create table accounts (aid int primary key, bid int, abalance int, filler text) with (type = \"hash\")",
		"create table history (tid int, bid int, aid int, delta int, time timestamp, seq int primary key serial, filler text) with (type = \"hash\")",
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
		buf_printf(buf, "INSERT INTO branches VALUES (%d, 0, \"%.*s\")",
		           i,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	for (auto i = 0; i < tpcb_tellers * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO tellers VALUES (%d, %d, 0, \"%.*s\")",
		           i, i / tpcb_tellers,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	for (auto i = 0; i < tpcb_accounts * scale; i++)
	{
		buf_reset(buf);
		buf_printf(buf, "INSERT INTO accounts VALUES (%d, %d, 0, \"%.*s\")",
		           i, i / tpcb_accounts,
		           buf_size(filler), filler->start);
		buf_str(buf, &str);
		client_execute(client, &str);
	}

	// add 100 to account 1 by teller 1 at branch 1
	tpcb_execute(client, filler, 1, 1, 1, 100);

	info("done.");
	info("");
}

static void
bench_tpcb_cleanup(Bench* self, Client* client)
{
	char* ddl[] =
	{
		"drop table branches",
		"drop table tellers",
		"drop table accounts",
		"drop table history",
		 NULL
	};
	for (auto i = 0; ddl[i]; i++)
	{
		Str str;
		str_set_cstr(&str, ddl[i]);
		client_execute(client, &str);
	}
	unused(self);
}

hot static void
bench_tpcb_main(BenchWorker* self, Client* client)
{
	auto bench  = self->bench;

	auto filler = buf_create();
	guard_buf(filler);
	buf_claim(filler, 100);
	memset(filler->start, ' ', 100);

	int  scale    = var_int_of(&bench->scale);
	auto accounts = tpcb_accounts * scale;
	auto branches = tpcb_branches * scale;
	auto tellers  = tpcb_tellers  * scale;

	while (! self->shutdown)
	{
		uint64_t random = random_generate(global()->random); 
		uint32_t a = *(uint32_t*)(&random);
		uint32_t b = *(uint32_t*)((uint8_t*)&random + sizeof(uint32_t));
		uint32_t c = a ^ b;
		int aid    = (a - 1) / accounts + 1;
		int bid    = (b - 1) / branches + 1;
		int tid    = (c - 1) / tellers + 1;
		int delta  = -5000 + (c % 10000); // -5000 to 5000

		tpcb_execute(client, filler, bid, tid, aid, delta);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, 4);
	}
}

BenchIf bench_tpcb =
{
	.init    = bench_tpcb_init,
	.cleanup = bench_tpcb_cleanup,
	.main    = bench_tpcb_main
};
