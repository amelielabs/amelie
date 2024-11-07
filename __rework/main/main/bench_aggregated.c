
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
	             "    a agg(count) as ( a::agg_count(1) ) aggregated,"
	             "    b agg(count) as ( b::agg_count(1) ) aggregated,"
	             "    c agg(count) as ( c::agg_count(1) ) aggregated,"
	             "    d agg(count) as ( d::agg_count(1) ) aggregated,"
	             "    e agg(count) as ( e::agg_count(1) ) aggregated"
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
