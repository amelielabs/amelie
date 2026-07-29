
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
bench_insert_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table test (id uuid primary key using hash identity)");
	client_execute(client, &str, NULL);

	auto batch = opt_int_of(&self->batch);
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
	client_execute(client, &cmd, NULL);
}

hot static void
bench_insert_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = opt_int_of(&bench->batch);

	Str cmd;
	str_set_cstr(&cmd,  "execute insert_func();");
	while (! self->shutdown)
	{
		client_execute(client, &cmd, NULL);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_insert =
{
	.create = bench_insert_create,
	.main   = bench_insert_main
};
