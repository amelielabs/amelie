
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

#include <amelie_test>

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
	format(path, sizeof(path), "{str}/__api0", &suite->option_result_dir);

	int   argc   = 9;
	char* argv[] =
	{
		"--log_stdout=false",
		"--wal_sync_create=false",
		"--wal_sync_close=false",
		"--wal_sync_write=false",
		"--wal_service=false",
		"--storage_sync=false",
		"--frontends=1",
		"--backends=1",
		"--listen=[]"
	};
	auto amelie = amelie_init();
	auto rc = amelie_open(amelie, path, argc, argv);
	test(! rc);
	amelie_free(amelie);
}
