
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
bench_resolved_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str,
	             "create table __bench.test ("
	             "    ts   timestamp as ( current_timestamp::date_bin('3 sec'::interval) ) stored,"
	             "    id   int random (10000),"
	             "    hits int default 0 as ( hits + 1 ) resolved,"
	             "    primary key(ts, id)"
	             ") with (type = 'hash')");
	client_execute(client, &str);
	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table __bench.test set unlogged");
		client_execute(client, &str);
	}
}

hot static void
bench_resolved_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = opt_int_of(&bench->batch);

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
