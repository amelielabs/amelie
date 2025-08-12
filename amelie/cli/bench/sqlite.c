
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
#include <sqlite3.h>

extern BenchClientIf bench_client_sqlite;

typedef struct BenchClientSqlite BenchClientSqlite;

struct BenchClientSqlite
{
	BenchClient obj;
	sqlite3*    db;
};

static BenchClient*
bench_client_sqlite_create(void* arg)
{
	unused(arg);
	auto self = (BenchClientSqlite*)am_malloc(sizeof(BenchClientSqlite));
	self->obj.iface = &bench_client_sqlite;
	self->obj.histogram = NULL;
	self->db = NULL;
	return &self->obj;
}

static void
bench_client_sqlite_free(BenchClient* ptr)
{
	auto self = (BenchClientSqlite*)ptr;
	if (self->db)
		sqlite3_close(self->db);
	am_free(ptr);
}

static void
bench_client_sqlite_connect(BenchClient* ptr, Remote* remote)
{
	unused(remote);
	auto self = (BenchClientSqlite*)ptr;
	auto flags = /* SQLITE_OPEN_MEMORY| */ SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_NOMUTEX;
	auto rc = sqlite3_open_v2("./sqlite", &self->db, flags, NULL);
	if (rc != SQLITE_OK)
		error("sqlite3_open() failed");

	char* err_msg = NULL;
	rc = sqlite3_exec(self->db, "PRAGMA synchronous=0", 0, 0, &err_msg);
	if (rc != SQLITE_OK)
		info("error: sqlite3_exec(): %s", err_msg);

	/*
	rc = sqlite3_exec(self->db, "PRAGMA journal_mode=off", 0, 0, &err_msg);
	if (rc != SQLITE_OK)
		info("error: sqlite3_exec(): %s", err_msg);
		*/

}

static void
bench_client_sqlite_set(BenchClient* ptr, Histogram* histogram)
{
	auto self = (BenchClientSqlite*)ptr;
	self->obj.histogram = histogram;
}

hot static inline void
sqlite_execute(BenchClientSqlite* self, Str* cmd)
{
	char* err_msg = NULL;
	auto rc = sqlite3_exec(self->db, cmd->pos, 0, 0, &err_msg);
	if (rc != SQLITE_OK)
		info("error: sqlite3_exec(): %s", err_msg);
}

hot static void
bench_client_sqlite_execute(BenchClient* ptr, Str* cmd)
{
	auto self = (BenchClientSqlite*)ptr;
	if (! self->obj.histogram)
	{
		sqlite_execute(self, cmd);
		return;
	}
	uint64_t time_us;
	time_start(&time_us);
	sqlite_execute(self, cmd);
	time_end(&time_us);
	histogram_add(self->obj.histogram, time_us / 1000);
}

hot static void
bench_client_sqlite_import(BenchClient* ptr, Str* path, Str* content_type, Str* content)
{
	unused(ptr);
	unused(path);
	unused(content_type);
	unused(content);
	error("import operation is not support with embeddable db");
}

BenchClientIf bench_client_sqlite =
{
	.create  = bench_client_sqlite_create,
	.free    = bench_client_sqlite_free,
	.set     = bench_client_sqlite_set,
	.connect = bench_client_sqlite_connect,
	.execute = bench_client_sqlite_execute,
	.import  = bench_client_sqlite_import
};
