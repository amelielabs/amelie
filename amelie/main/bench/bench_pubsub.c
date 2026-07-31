
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
bench_pubsub_create(Bench* self, Client* client)
{
	auto batch = (int)opt_int_of(&self->batch);

	info("preparing topic.");
	Str str;
	str_set_cstr(&str, "create topic bench_topic");
	client_execute(client, &str, NULL);

	info("preparing subscription.");
	str_set_cstr(&str, "create subscription bench_sub on bench_topic");
	client_execute(client, &str, NULL);

	info("preparing function.");
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	buf_format(&buf,
	           "create function publish_func() "
	           "begin "
	           "  PUBLISH INTO bench_topic ");
	for (int i = 0; i < batch; i++)
		buf_format(&buf, "{s}1", i > 0 ? "," : "");
	buf_format(&buf, ";");
	buf_format(&buf, "end");
	buf_str(&buf, &str);
	client_execute(client, &str, NULL);

	info("done.");
	info("");
}

hot static void
bench_pubsub_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = (int)opt_int_of(&bench->batch);

	Str cmd;
	str_set_cstr(&cmd,  "execute publish_func();");

	while (! self->shutdown)
	{
		client_execute(client, &cmd, NULL);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_pubsub =
{
	.create = bench_pubsub_create,
	.main   = bench_pubsub_main
};
