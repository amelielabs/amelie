
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
bench_resolved_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str,
	             "create table __bench.test ("
	             "    id int primary key random (100000),"
	             "    a int as ( a + 1 ) resolved default 0,"
	             "    b int as ( b + 1 ) resolved default 0,"
	             "    c int as ( c + 1 ) resolved default 0,"
	             "    d int as ( d + 1 ) resolved default 0,"
	             "    e int as ( e + 1 ) resolved default 0"
	             ") with (type = 'hash')");
	client_execute(client, &str);
}

hot static void
bench_resolved_main(BenchWorker* self, Client* client)
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

BenchIf bench_resolved =
{
	.create = bench_resolved_create,
	.main   = bench_resolved_main
};
