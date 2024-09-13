
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

hot static void
bench_upsert_send(Bench* self, Client* client)
{
	client_send(client, &self->command);
}

hot static void
bench_upsert_recv(Bench* self, Client* client)
{
	client_recv(client);

	atomic_u64_add(&self->transactions, 1);
	atomic_u64_add(&self->writes, var_int_of(&self->batch));
}

static void
bench_upsert_create(Bench* self, Client* client)
{
	auto batch = var_int_of(&self->batch);
	char text[256];
	snprintf(text, sizeof(text),
	         "insert into __bench.test "
	         "generate %" PRIu64 " "
	         "on conflict do update set data = data + 1",
	         batch);
	str_strdup(&self->command, text);

	Str str;
	str_set_cstr(&str, "create table __bench.test (id int primary key random (100000), data int default 0) with (type = \"hash\")");
	client_execute(client, &str);
}

BenchIf bench_upsert =
{
	.send   = bench_upsert_send,
	.recv   = bench_upsert_recv,
	.create = bench_upsert_create
};
