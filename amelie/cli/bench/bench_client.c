
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
#include <amelie.h>
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
	self->obj.histogram = NULL;
	self->client = client_create();
	return &self->obj;
}

static void
bench_client_http_free(BenchClient* ptr)
{
	auto self = (BenchClientHttp*)ptr;
	client_close(self->client);
	client_free(self->client);
	am_free(ptr);
}

static void
bench_client_http_set(BenchClient* ptr, Histogram* histogram)
{
	auto self = (BenchClientHttp*)ptr;
	self->obj.histogram = histogram;
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
	if (! self->obj.histogram)
	{
		client_execute(self->client, cmd);
		return;
	}

	uint64_t time_us;
	time_start(&time_us);
	client_execute(self->client, cmd);
	time_end(&time_us);
	histogram_add(self->obj.histogram, time_us);
}

hot static void
bench_client_http_import(BenchClient* ptr, Str* path, Str* content_type, Str* content)
{
	auto self = (BenchClientHttp*)ptr;
	if (! self->obj.histogram)
	{
		client_import(self->client, path, content_type, content);
		return;
	}

	uint64_t time_us;
	time_start(&time_us);
	client_import(self->client, path, content_type, content);
	time_end(&time_us);
	histogram_add(self->obj.histogram, time_us);
}

BenchClientIf bench_client_http =
{
	.create  = bench_client_http_create,
	.free    = bench_client_http_free,
	.set     = bench_client_http_set,
	.connect = bench_client_http_connect,
	.execute = bench_client_http_execute,
	.import  = bench_client_http_import
};

typedef struct BenchClientApi BenchClientApi;

struct BenchClientApi
{
	BenchClient       obj;
	amelie_session_t* session;
	amelie_t*         amelie;
};

static BenchClient*
bench_client_api_create(void* arg)
{
	unused(arg);
	auto self = (BenchClientApi*)am_malloc(sizeof(BenchClientApi));
	self->obj.iface = &bench_client_api;
	self->session   = NULL;
	self->amelie    = arg;
	return &self->obj;
}

static void
bench_client_api_free(BenchClient* ptr)
{
	auto self = (BenchClientApi*)ptr;
	if (self->session)
		amelie_free(self->session);
	am_free(ptr);
}

static void
bench_client_api_connect(BenchClient* ptr, Remote* remote)
{
	unused(remote);
	auto self = (BenchClientApi*)ptr;
	self->session = amelie_connect(self->amelie);
	if (unlikely(! self->session))
		error("amelie_connect() failed");
}

static void
bench_client_api_set(BenchClient* ptr, Histogram* histogram)
{
	auto self = (BenchClientApi*)ptr;
	self->obj.histogram = histogram;
}

hot static void
bench_client_api_execute(BenchClient* ptr, Str* cmd)
{
	auto self = (BenchClientApi*)ptr;
	if (! self->obj.histogram)
	{
		amelie_execute(self->session, cmd->pos, 0, NULL, NULL);
		return;
	}

	uint64_t time_us;
	time_start(&time_us);
	amelie_execute(self->session, cmd->pos, 0, NULL, NULL);
	time_end(&time_us);
	histogram_add(self->obj.histogram, time_us);
}

hot static void
bench_client_api_import(BenchClient* ptr, Str* path, Str* content_type, Str* content)
{
	unused(ptr);
	unused(path);
	unused(content_type);
	unused(content);
	error("import operation is not support with embeddable db");
}

BenchClientIf bench_client_api =
{
	.create  = bench_client_api_create,
	.free    = bench_client_api_free,
	.set     = bench_client_api_set,
	.connect = bench_client_api_connect,
	.execute = bench_client_api_execute,
	.import  = bench_client_api_import
};
