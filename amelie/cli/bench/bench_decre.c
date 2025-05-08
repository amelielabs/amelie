
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

	auto text =
	"create procedure __bench.decre(a int, b int, amount double) "
	"begin "
	" update __bench.test set money = money - amount where id = a; "
	" update __bench.test set money = money + amount where id = b; "
	"end ";
	str_set_cstr(&str, text);
	client_execute(client, &str);
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
			uint32_t a = *(uint32_t*)(&random);
			uint32_t b = *(uint32_t*)((uint8_t*)&random + sizeof(uint32_t));
			int from = a % total;
			int to   = b % total;
			buf_printf(&buf, "CALL __bench.decre(%d, %d, 1.0);", from, to);
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
