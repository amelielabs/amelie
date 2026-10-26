
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
bench_decre_deploy(BenchClient* self)
{
	auto client = self->client;
	unused(self);

	Str str;
	str_set_cstr(&str, "create table accounts(id int primary key using hash, money decimal default 100.0)");
	client_execute(client, &str, NULL);

	str_set_cstr(&str, "create table history(src int, dst int, amount decimal)");
	client_execute(client, &str, NULL);

	// prepare dataset
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	auto n = 100000ul / 500;
	for (auto i = 0ul; i < n; i++)
	{
		buf_reset(&buf);
		buf_format(&buf, "insert into accounts (id) values ");
		for (uint64_t j = 0; j < 500; j++)
		{
			buf_format(&buf, "({d})", i * 500 + j);
			if ((j + 1) != 500)
				buf_write(&buf, ",", 1);
		}
		buf_str(&buf, &str);
		client_execute(client, &str, NULL);
	}

	// create benchmark functions
	char func[] =
	"create function debit_credit(src int, dst int, amount decimal)"
	"begin"
	"	update accounts set money = money - amount"
	"	 where id = src;"
	""
	"	update accounts set money = money + amount"
	"	 where id = dst;"
	""
	"	insert into history (src, dst, amount)"
	"	values (src, dst, amount);"
	"end;";

	char func_batch[] =
	"create function debit_credit_batch(batch int, total int)"
	"begin"
	"	while batch > 0 do"
	"		select debit_credit(random() \% total, random() \% total, decimal '1.0');"
	"		batch := batch - 1;"
	"	end;"
	"end;";

	str_set_cstr(&str, func);
	client_execute(client, &str, NULL);

	str_set_cstr(&str, func_batch);
	client_execute(client, &str, NULL);
}

hot static void
bench_decre_main(BenchClient* self)
{
	auto bench  = self->bench;
	auto total  = 100000;
	auto batch  = (int)opt_int_of(&bench->opt_batch);
	auto client = self->client;

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_format(&buf, "execute debit_credit_batch({d}, {d});", batch, total);
	Str cmd;
	buf_str(&buf, &cmd);
	while (! bench->shutdown)
	{
		client_execute(client, &cmd, NULL);
		bench->transactions += batch;
		bench->writes += 3 * batch;
	}
}

BenchIf bench_decre =
{
	.deploy = bench_decre_deploy,
	.main   = bench_decre_main
};
