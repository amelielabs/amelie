
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

#include <amelie_private.h>

static void
bench_aggregated_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str,
	             "create aggregated table __bench.test ("
	             "    id int primary key random (100000),"
	             "    a int as ( a + 1 ) aggregated default 0,"
	             "    b int as ( b + 1 ) aggregated default 0,"
	             "    c int as ( c + 1 ) aggregated default 0,"
	             "    d int as ( d + 1 ) aggregated default 0,"
	             "    e int as ( e + 1 ) aggregated default 0"
	             ") with (type = 'hash')");
	client_execute(client, &str);
}

hot static void
bench_aggregated_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = var_int_of(&bench->batch);

	char text[256];
	snprintf(text, sizeof(text), "insert into __bench.test generate %" PRIu64, batch);
	Str cmd;
	str_set_cstr(&cmd, text);

	while (! self->shutdown)
	{
		client_execute(client, &cmd);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_aggregated =
{
	.create = bench_aggregated_create,
	.main   = bench_aggregated_main
};
