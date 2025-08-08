
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

#include <amelie_core.h>
#include <amelie_cli.h>
#include <amelie_cli_bench.h>

typedef struct BenchClientHttp BenchClientHttp;

struct BenchClientHttp
{
	BenchClient obj;
	Client*     client;
};

static BenchClient*
bench_client_http_create(void* arg)
{
	unused(arg);
	auto self = (BenchClientHttp*)am_malloc(sizeof(BenchClientHttp));
	self->obj.iface = &bench_client_http;
	self->client = client_create();
	return &self->obj;
}

static void
bench_client_http_free(BenchClient* ptr)
{
	auto self = (BenchClientHttp*)ptr;
	if (self->client)
	{
		client_close(self->client);
		client_free(self->client);
	}
	am_free(ptr);
}

static void
bench_client_http_connect(BenchClient* ptr, Remote* remote)
{
	auto self = (BenchClientHttp*)ptr;
	client_set_remote(self->client, remote);
	client_connect(self->client);
}

hot static void
bench_client_http_execute(BenchClient* ptr, Str* cmd)
{
	auto self = (BenchClientHttp*)ptr;
	client_execute(self->client, cmd);
}

hot static void
bench_client_http_import(BenchClient* ptr, Str* path, Str* content_type, Str* content)
{
	auto self = (BenchClientHttp*)ptr;
	client_import(self->client, path, content_type, content);
}

BenchClientIf bench_client_http =
{
	.create  = bench_client_http_create,
	.free    = bench_client_http_free,
	.connect = bench_client_http_connect,
	.execute = bench_client_http_execute,
	.import  = bench_client_http_import
};
