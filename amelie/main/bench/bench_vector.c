
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
#include <math.h>

static const int vector_dim = 128;
static const int vector_rows = 100000;

typedef struct VectorLoader VectorLoader;

struct VectorLoader
{
	int       from;
	int       to;
	int       batch;
	uint64_t* transactions;
	uint64_t* writes;
	int*      complete;
	Bench*    bench;
};

static void
vector_loader_client_main(VectorLoader* self, MainClient* client)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	for (auto seq = self->from; seq < self->to;)
	{
		int batch = 0;
		if (self->to - seq >= self->batch)
			batch = self->batch;
		else
			batch = self->to - seq;

		buf_reset(&buf);
		buf_format(&buf, "INSERT INTO bench_vector VALUES ");
		for (int i = 0; i < batch; i++)
		{
			buf_format(&buf, "{s}({d}, vector [", i > 0 ? "," : "", seq);
			for (int j = 0; j < vector_dim; j++)
			{
				float val = (float)random_generate(&am_task->random) / (float)UINT64_MAX;
				buf_format(&buf, "{s}{.4f}", j > 0 ? "," : "", val);
			}
			buf_format(&buf, "])");
			seq++;
		}
		Str str;
		buf_str(&buf, &str);
		main_client_execute(client, &str, NULL);

		(*self->writes) += batch;
		(*self->transactions)++;
	}
}

static void
vector_loader_main(void* arg)
{
	auto self   = (VectorLoader*)arg;
	auto client = main_client_create(self->bench->main);
	defer(main_client_free, client);
	error_catch
	(
		main_client_connect(client);
		vector_loader_client_main(self, client);
	);

	(*self->complete)++;
}

static void
bench_vector_load(Bench* self, int scale, int batch, int clients)
{
	auto loaders_complete = 0;
	auto loaders_count = clients;
	VectorLoader* loaders = am_malloc(sizeof(VectorLoader) * loaders_count);
	defer(am_free, loaders);

	uint64_t transactions = 0;
	uint64_t writes       = 0;
	auto     rows         = vector_rows * scale;
	auto     step         = rows / loaders_count;
	auto     from         = 0ul;
	for (auto i = 0; i < loaders_count; i++)
	{
		auto next = 0ul;
		if ((i + 1) < loaders_count)
			next = step;
		else
			next = rows - from;
		auto loader = &loaders[i];
		loader->from         = from;
		loader->to           = from + next;
		loader->batch        = batch;
		loader->transactions = &transactions;
		loader->writes       = &writes;
		loader->complete     = &loaders_complete;
		loader->bench        = self;
		coroutine_create(vector_loader_main, loader);
		from += next;
	}

	uint64_t prev_tx = 0;
	uint64_t prev_wr = 0;
	while (loaders_complete != loaders_count)
	{
		coroutine_sleep(1000);
		auto tx = transactions;
		auto wr = writes;
		info("{u64} transactions/sec, {.2f} millions writes/sec {u64}%",
		     tx - prev_tx,
		     (float)(wr - prev_wr) / 1000000.0,
		     (wr * 100ul) / rows);
		prev_tx = tx;
		prev_wr = wr;
	}
}

static void
bench_vector_create(Bench* self, MainClient* client)
{
	info("preparing tables.");
	Str str;
	str_set_cstr(&str, "create table bench_vector (id int primary key, v vector)");
	main_client_execute(client, &str, NULL);

	info("preparing data.");
	auto batch   = (int)opt_int_of(&self->batch);
	auto scale   = (int)opt_int_of(&self->scale);
	auto clients = opt_int_of(&self->clients);
	bench_vector_load(self, scale, batch, clients);

	info("done.");
	info("");
}

hot static void
bench_vector_main(BenchWorker* self, MainClient* client)
{
	auto bench = self->bench;
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	while (! self->shutdown)
	{
		buf_reset(&buf);
		buf_format(&buf, "SELECT id FROM bench_vector ORDER BY v::cos_distance(vector [");
		for (int j = 0; j < vector_dim; j++)
		{
			float val = (float)random_generate(&am_task->random) / (float)UINT64_MAX;
			buf_format(&buf, "{s}{.4f}", j > 0 ? "," : "", val);
		}
		buf_format(&buf, "]) ASC LIMIT 10");

		Str cmd;
		buf_str(&buf, &cmd);
		main_client_execute(client, &cmd, NULL);

		atomic_u64_add(&bench->transactions, 1);
	}
}

BenchIf bench_vector =
{
	.create = bench_vector_create,
	.main   = bench_vector_main
};
