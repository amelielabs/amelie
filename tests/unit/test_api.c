
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

#include <amelie_test.h>
#include <amelie.h>

void
test_api_init(void* arg)
{
	(void)arg;
	auto amelie = amelie_init();
	amelie_free(amelie);
}

void
test_api_open(void* arg)
{
	TestSuite* suite = arg;

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/__api0", str_size(&suite->option_result_dir),
	         str_of(&suite->option_result_dir));

	int   argc   = 9;
	char* argv[] =
	{
		"--log_to_stdout=false",
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--checkpoint_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	auto amelie = amelie_init();
	auto rc = amelie_open(amelie, path, argc, argv);
	test(! rc);
	amelie_free(amelie);
}

void
test_api_connect(void* arg)
{
	TestSuite* suite = arg;

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/__api1", str_size(&suite->option_result_dir),
	         str_of(&suite->option_result_dir));

	int   argc   = 9;
	char* argv[] =
	{
		"--log_to_stdout=false",
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--checkpoint_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	auto amelie = amelie_init();
	auto rc = amelie_open(amelie, path, argc, argv);
	test(! rc);

	auto session1 = amelie_connect(amelie, NULL);
	test(session1);
	amelie_free(session1);

	auto session2 = amelie_connect(amelie, NULL);
	test(session2);
	amelie_free(session2);

	amelie_free(amelie);
}

void
test_api_execute(void* arg)
{
	TestSuite* suite = arg;

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/__api2", str_size(&suite->option_result_dir),
	         str_of(&suite->option_result_dir));

	int   argc   = 9;
	char* argv[] =
	{
		"--log_to_stdout=false",
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--checkpoint_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	auto amelie = amelie_init();
	auto rc = amelie_open(amelie, path, argc, argv);
	test(! rc);

	auto session = amelie_connect(amelie, NULL);
	test(session);

	auto req = amelie_execute(session, "create table test (id int primary key)", 0, NULL, NULL, NULL);
	test(amelie_wait(req, -1, NULL) == 204);
	// wait twice test
	test(amelie_wait(req, -1, NULL) == 204);
	amelie_free(req);

	req = amelie_execute(session, "insert into test values (1), (2), (3)", 0, NULL, NULL, NULL);
	test(amelie_wait(req, -1, NULL) == 204);
	amelie_free(req);

	req = amelie_execute(session, "select * from test", 0, NULL, NULL, NULL);
	amelie_arg_t result = {
		.data = NULL,
		.data_size = 0
	};
	test(amelie_wait(req, -1, &result) == 200);
	test(result.data_size == 9);
	test(! memcmp(result.data, "[1, 2, 3]", 9));
	amelie_free(req);

	amelie_free(session);
	amelie_free(amelie);

	// restart and retry
	amelie = amelie_init();
	rc = amelie_open(amelie, path, argc, argv);
	test(! rc);

	session = amelie_connect(amelie, NULL);
	test(session);

	result.data = NULL;
	result.data_size = 0;
	req = amelie_execute(session, "select * from test", 0, NULL, NULL, NULL);
	test(amelie_wait(req, -1, &result) == 200);
	test(result.data_size == 9);
	test(! memcmp(result.data, "[1, 2, 3]", 9));
	amelie_free(req);

	amelie_free(session);
	amelie_free(amelie);
}

void
test_api_execute_error(void* arg)
{
	TestSuite* suite = arg;

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/__api3", str_size(&suite->option_result_dir),
	         str_of(&suite->option_result_dir));

	int   argc   = 9;
	char* argv[] =
	{
		"--log_to_stdout=false",
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--checkpoint_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	auto amelie = amelie_init();
	auto rc = amelie_open(amelie, path, argc, argv);
	test(! rc);

	auto session = amelie_connect(amelie, NULL);
	test(session);

	auto req = amelie_execute(session, "select abcd", 0, NULL, NULL, NULL);
	test(amelie_wait(req, -1, NULL) == 400);
	amelie_free(req);

	amelie_free(session);
	amelie_free(amelie);
}
