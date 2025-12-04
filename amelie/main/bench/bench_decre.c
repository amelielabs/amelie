
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

static void
bench_decre_create(Bench* self, MainClient* client)
{
	unused(self);

	Str str;
	str_set_cstr(&str, "create table test(id serial primary key using hash, money double default 100.0)");
	main_client_execute(client, &str, NULL);

	str_set_cstr(&str, "create table history(id serial primary key using hash, src int, dst int, amount double)");
	main_client_execute(client, &str, NULL);

	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table test set unlogged");
		main_client_execute(client, &str, NULL);
		str_set_cstr(&str, "alter table history set unlogged");
		main_client_execute(client, &str, NULL);
	}
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	auto n = 100000ul / 500;
	for (auto i = 0ul; i < n; i++)
	{
		buf_reset(&buf);
		buf_printf(&buf, "insert into test generate 500");
		buf_str(&buf, &str);
		main_client_execute(client, &str, NULL);
	}

	// create benchmark functions
	char func[] =
	"create function debit_credit(src int, dst int, amount double)"
	"begin"
	"	update test set money = money - amount"
	"	 where id = src;"
	""
	"	update test set money = money + amount"
	"	 where id = dst;"
	""
	"	insert into history (src, dst, amount)"
	"	values (src, dst, amount);"
	"end;";

	char func_batch[] =
	"create function debit_credit_batch(batch int, total int)"
	"begin"
	"	while batch > 0 do"
	"		select debit_credit(random() \% total, random() \% total, 1.0);"
	"		batch := batch - 1;"
	"	end;"
	"end;";

	str_set_cstr(&str, func);
	main_client_execute(client, &str, NULL);

	str_set_cstr(&str, func_batch);
	main_client_execute(client, &str, NULL);
}

hot static void
bench_decre_main(BenchWorker* self, MainClient* client)
{
	auto bench = self->bench;
	auto total = 100000;
	auto batch = (int)opt_int_of(&bench->batch);

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_printf(&buf, "execute debit_credit_batch(%d, %d);", batch, total);
	Str cmd;
	buf_str(&buf, &cmd);

	while (! self->shutdown)
	{
		main_client_execute(client, &cmd, NULL);
		atomic_u64_add(&bench->transactions, batch);
		atomic_u64_add(&bench->writes, 3 * batch);
	}
}

BenchIf bench_decre =
{
	.create = bench_decre_create,
	.main   = bench_decre_main
};
