
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

static void
bench_decre_create(Bench* self, Client* client)
{
	unused(self);

	Str str;
	str_set_cstr(&str, "create table __bench.test (id serial primary key, money double default 100.0) with (type = 'hash')");
	client_execute(client, &str);
	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table __bench.test set unlogged");
		client_execute(client, &str);
	}
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	auto n = 100000ul / 500;
	for (auto i = 0ul; i < n; i++)
	{
		buf_reset(&buf);
		buf_printf(&buf, "insert into __bench.test generate 500");
		buf_str(&buf, &str);
		client_execute(client, &str);
	}
}

hot static inline void
decre_transaction(Buf* buf, int from, int to, double amount)
{
	buf_printf(buf, "UPDATE __bench.test SET money = money - %f WHERE id = %d;",
	           amount, from);
	buf_printf(buf, "UPDATE __bench.test SET money = money + %f WHERE id = %d;",
	           amount, to);
}

hot static void
bench_decre_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto total = 100000ul;
	auto batch = opt_int_of(&bench->batch);

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	while (! self->shutdown)
	{
		buf_reset(&buf);
		for (auto i = 0ul; i < batch; i++)
		{
			uint64_t random = random_generate(global()->random);
			uint32_t a = random;
			uint32_t b = random >> 32;
			int from = a % total;
			int to   = b % total;
			decre_transaction(&buf, from, to, 1.0);
		}
		Str cmd;
		buf_str(&buf, &cmd);
		client_execute(client, &cmd);

		atomic_u64_add(&bench->transactions, batch);
		atomic_u64_add(&bench->writes, 2 * batch);
	}
}

BenchIf bench_decre =
{
	.create = bench_decre_create,
	.main   = bench_decre_main
};
