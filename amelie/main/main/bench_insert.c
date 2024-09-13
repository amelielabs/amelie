
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

hot static void
bench_insert_send(Bench* self, Client* client)
{
	client_send(client, &self->command);
}

hot static void
bench_insert_recv(Bench* self, Client* client)
{
	client_recv(client);

	atomic_u64_add(&self->transactions, 1);
	atomic_u64_add(&self->writes, var_int_of(&self->batch));
}

static void
bench_insert_create(Bench* self, Client* client)
{
	auto batch = var_int_of(&self->batch);
	char text[256];
	snprintf(text, sizeof(text), "insert into __bench.test generate %" PRIu64, batch);
	str_strdup(&self->command, text);

	Str str;
	str_set_cstr(&str, "create table __bench.test (id int primary key serial)");
	client_execute(client, &str);
}

BenchIf bench_insert =
{
	.send   = bench_insert_send,
	.recv   = bench_insert_recv,
	.create = bench_insert_create
};
