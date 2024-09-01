
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

static void
bench_insert_init(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table __test (id int primary key serial)");
	client_execute(client, &str);
}

static void
bench_insert_cleanup(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "drop table __test");
	client_execute(client, &str);
}

hot static void
bench_insert_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = var_int_of(&bench->batch);

	char text[256];
	snprintf(text, sizeof(text), "insert into __test generate %" PRIu64, batch);
	Str cmd;
	str_set_cstr(&cmd, text);

	while (! self->shutdown)
	{
		client_execute(client, &cmd);
		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_insert =
{
	.init    = bench_insert_init,
	.cleanup = bench_insert_cleanup,
	.main    = bench_insert_main
};
