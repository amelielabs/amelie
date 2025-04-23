
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
bench_import_create(Bench* self, Client* client)
{
	unused(self);
	Str str;
	str_set_cstr(&str, "create table __bench.test (id serial primary key)");
	client_execute(client, &str);
	if (opt_int_of(&self->unlogged))
	{
		str_set_cstr(&str, "alter table __bench.test set unlogged");
		client_execute(client, &str);
	}
}

hot static void
bench_import_main(BenchWorker* self, Client* client)
{
	auto bench = self->bench;
	auto batch = opt_int_of(&bench->batch);

	// path
	Str path;
	str_set_cstr(&path, "/v1/db/__bench/test?columns=");

	// content type
	Str content_type;
	str_set_cstr(&content_type, "application/json");

	// content

	// [[], ...]
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
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

	while (! self->shutdown)
	{
		client_import(client, &path, &content_type, &content);

		atomic_u64_add(&bench->transactions, 1);
		atomic_u64_add(&bench->writes, batch);
	}
}

BenchIf bench_import =
{
	.create = bench_import_create,
	.main   = bench_import_main
};
