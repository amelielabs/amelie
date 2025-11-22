
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_bench.h>

static void
bench_decre_create(Bench* self, BenchClient* client)
{
	unused(self);

	Str str;
	str_set_cstr(&str, "create table __bench.test (id serial primary key using hash, money double default 100.0)");
	bench_client_execute(client, &str);

	str_set_cstr(&str, "create table __bench.history (id serial primary key using hash, src int, dst int, amount double)");
	bench_client_execute(client, &str);

	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table __bench.test set unlogged");
		bench_client_execute(client, &str);
		str_set_cstr(&str, "alter table __bench.history set unlogged");
		bench_client_execute(client, &str);
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
		bench_client_execute(client, &str);
	}

	// create benchmark functions
	char func[] =
	"create function __bench.debit_credit(src int, dst int, amount double)"
	"begin"
	"	update __bench.test set money = money - amount"
	"	 where id = src;"
	""
	"	update __bench.test set money = money + amount"
	"	 where id = dst;"
	""
	"	insert into __bench.history (src, dst, amount)"
	"	values (src, dst, amount);"
	"end;";

	char func_batch[] =
	"create function __bench.debit_credit_batch(batch int, total int)"
	"begin"
	"	while batch > 0 do"
	"		select __bench.debit_credit(random() \% total, random() \% total, 1.0);"
	"		batch := batch - 1;"
	"	end;"
	"end;";

	str_set_cstr(&str, func);
	bench_client_execute(client, &str);

	str_set_cstr(&str, func_batch);
	bench_client_execute(client, &str);
}

hot static void
bench_decre_main(BenchWorker* self, BenchClient* client)
{
	auto bench = self->bench;
	auto total = 100000ul;
	auto batch = opt_int_of(&bench->batch);

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_printf(&buf, "execute __bench.debit_credit_batch(%d, %d);", batch, total);
	Str cmd;
	buf_str(&buf, &cmd);

	while (! self->shutdown)
	{
		bench_client_execute(client, &cmd);
		atomic_u64_add(&bench->transactions, batch);
		atomic_u64_add(&bench->writes, 3 * batch);
	}
}

BenchIf bench_decre =
{
	.create = bench_decre_create,
	.main   = bench_decre_main
};
