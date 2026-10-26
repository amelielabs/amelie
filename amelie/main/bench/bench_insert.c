
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
bench_insert_deploy(BenchClient* self)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table test (id int) retention 1gb");
	client_execute(self->client, &str, NULL);

	auto batch = opt_int_of(&self->bench->opt_batch);
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_format(&buf,
	           "create function insert_func() "
	           "begin "
	           "  insert into test () values ");
	for (uint64_t i = 0; i < batch; i++)
	{
		buf_write(&buf, "()", 2);
		if ((i + 1) != batch)
			buf_write(&buf, ",", 1);
	}
	buf_format(&buf, ";");
	buf_format(&buf, "end");

	Str cmd;
	buf_str(&buf, &cmd);
	client_execute(self->client, &cmd, NULL);
}

hot static void
bench_insert_main(BenchClient* self)
{
	auto bench  = self->bench;
	auto batch  = opt_int_of(&bench->opt_batch);
	auto client = self->client;

	Str cmd;
	str_set_cstr(&cmd,  "execute insert_func();");
	while (! bench->shutdown)
	{
		client_execute(client, &cmd, NULL);

		bench->transactions += 1;
		bench->writes += batch;
	}
}

BenchIf bench_insert =
{
	.deploy = bench_insert_deploy,
	.main   = bench_insert_main
};
