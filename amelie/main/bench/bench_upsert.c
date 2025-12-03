
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
#include <amelie_main.h>
#include <amelie_main_bench.h>

static void
bench_upsert_create(Bench* self, MainClient* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table test(id int primary key using hash as identity random (100000), data int default 0)");
	main_client_execute(client, &str, NULL);
	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table test set unlogged");
		main_client_execute(client, &str, NULL);
	}
}

hot static void
bench_upsert_main(BenchWorker* self, MainClient* client)
{
	auto bench = self->bench;
	auto batch = opt_int_of(&bench->batch);

	char text[256];
	snprintf(text, sizeof(text),
	         "insert into test "
	         "generate %" PRIu64 " "
	         "on conflict do update set data = data + 1",
	         batch);
	Str cmd;
	str_set_cstr(&cmd, text);

	while (! self->shutdown)
	{
		main_client_execute(client, &cmd, NULL);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_upsert =
{
	.create = bench_upsert_create,
	.main   = bench_upsert_main
};
