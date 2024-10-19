
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_private.h>

static void
bench_import_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table __bench.test (id int primary key serial)");
	client_execute(client, &str);
}

hot static void
bench_import_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = var_int_of(&bench->batch);

	// [[], ...]
	Buf buf;
	buf_init(&buf);
	guard_buf(&buf);
	buf_write(&buf, "[", 1);
	for (uint64_t i = 0; i < batch; i++)
	{
		buf_write(&buf, "[]", 2);
		if ((i + 1) != batch)
			buf_write(&buf, ",", 1);
	}
	buf_write(&buf, "]", 1);
	Str content;
	buf_str(&buf, &content);

	Str path;
	str_set_cstr(&path, "/__bench/test?columns=");

	while (! self->shutdown)
	{
		client_import(client, &path, &content);

		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_import =
{
	.create = bench_import_create,
	.main   = bench_import_main
};
