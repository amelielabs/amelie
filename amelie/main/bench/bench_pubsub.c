
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
bench_pubsub_create(Bench* self, MainClient* client)
{
	unused(self);

	info("preparing topic.");
	Str str;
	str_set_cstr(&str, "create topic bench_topic");
	main_client_execute(client, &str, NULL);

	info("preparing subscription.");
	str_set_cstr(&str, "create subscription bench_sub on bench_topic");
	main_client_execute(client, &str, NULL);

	info("done.");
	info("");
}

hot static void
bench_pubsub_main(BenchWorker* self, MainClient* client)
{
	auto bench = self->bench;
	int batch = (int)opt_int_of(&bench->batch);
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// payload (~100 bytes)
	const char* payload = "{"
	                        "\"msg\": \"frontline coordination event\", "
	                        "\"status\": \"active\", "
	                        "\"priority\": \"high\", "
	                        "\"metadata\": { \"source\": \"agent_v1\" }"
	                      "}";
	while (! self->shutdown)
	{
		buf_reset(&buf);
		buf_format(&buf, "PUBLISH INTO bench_topic ");
		for (int i = 0; i < batch; i++)
			buf_format(&buf, "{s}{s}", i > 0 ? "," : "", payload);

		Str cmd;
		buf_str(&buf, &cmd);
		main_client_execute(client, &cmd, NULL);

		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_pubsub =
{
	.create = bench_pubsub_create,
	.main   = bench_pubsub_main
};
