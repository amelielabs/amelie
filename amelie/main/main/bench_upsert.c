
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

static void
bench_upsert_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table __bench.test (id int primary key random (100000), data int default 0) with (type = 'hash')");
	client_execute(client, &str);
}

hot static void
bench_upsert_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = var_int_of(&bench->batch);

	char text[256];
	snprintf(text, sizeof(text),
	         "insert into __bench.test "
	         "generate %" PRIu64 " "
	         "on conflict do update set data = data + 1",
	         batch);
	Str cmd;
	str_set_cstr(&cmd, text);

	while (! self->shutdown)
	{
		client_execute(client, &cmd);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_upsert =
{
	.create = bench_upsert_create,
	.main   = bench_upsert_main
};
