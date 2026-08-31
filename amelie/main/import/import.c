
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
#include <amelie_main_import.h>

void
import_init(Import* self, Main* main)
{
	self->errors           = 0;
	self->report_time      = 0;
	self->report_processed = 0;
	self->main             = main;
	self->forward          = NULL;
	csv_init(&self->csv);
	list_init(&self->clients_list);

	opts_init(&self->opts);
	OptsDef defs[] =
	{
		{ "batch",   OPT_INT, OPT_C|OPT_Z, &self->batch,   NULL,  128 * 1024 },
		{ "clients", OPT_INT, OPT_C|OPT_Z, &self->clients, NULL,  16         },
		{  NULL,     0,       0,            NULL,          NULL,  0          }
	};
	opts_define(&self->opts, defs);
}

void
import_free(Import* self)
{
	csv_free(&self->csv);
	opts_free(&self->opts);
}

static void
import_connect(Import* self)
{
	// create clients and connect
	int count = opt_int_of(&self->clients);
	while (count-- > 0)
	{
		auto client = client_allocate();
		list_append(&self->clients_list, &client->link);

		// set default content_type
		auto endpoint = &self->main->endpoint;
		auto content_type = opt_string_of(&endpoint->content_type);
		if (str_empty(content_type))
			opt_string_set_raw(&endpoint->content_type, "text/csv", 8);

		// create client and connect
		client_set_endpoint(client, endpoint);
		client_connect(client);
	}
}

static void
import_disconnect(Import* self)
{
	list_foreach_safe(&self->clients_list)
	{
		auto client = list_at(Client, link);
		client_free(client);
	}
}

static void
import_sync(Import* self, Client* client)
{
	while (client->sync > 0)
	{
		auto code = client_recv(client, NULL);
		if (code != 200 && code != 204)
			self->errors++;
		client->sync--;
	}
}

static void
import_sync_all(Import* self)
{
	list_foreach(&self->clients_list)
	{
		auto client = list_at(Client, link);
		import_sync(self, client);
	}
}

static void
import_send(Import* self, Str* content)
{
	Client* next;
	if (!self->forward || list_is_last(&self->clients_list, &self->forward->link))
	{
		auto first = list_first(&self->clients_list);
		next = container_of(first, Client, link);
	} else {
		next = container_of(self->forward->link.next, Client, link);
	}

	// read reply from previous request
	import_sync(self, next);

	// POST /import
	client_send(next, content);
	next->sync++;

	self->forward = next;
}

hot static inline void
import_report(Import* self, File* file, uint64_t processed)
{
	clock_reset(&am_task->clock);
	auto     time           = time_us();
	double   time_diff      = (time - self->report_time) / 1000.0 / 1000.0;
	uint64_t processed_diff = processed - self->report_processed;
	int      processed_sec  = 0;
	if (time_diff > 0)
		processed_sec = (int)(((double)processed_diff / 1024 / 1024) / time_diff);

	auto total   = file->size / 1024 / 1024;
	auto done    = processed / 1024 / 1024;
	int  percent = 0;
	if (total > 0)
		percent = (done * 100ull) / total;
	else
	if (total == 0)
		percent = 100;

	info("{str} {d}% ({d} Mb / {d} Mb) {d} Mb/sec, {u64} errors\r",
	     &file->path,
	     (int)percent, (int)done, (int)total,
	     processed_sec,
	     self->errors);

	fflush(stdout);

	self->report_time      = time;
	self->report_processed = processed;
}

static void
import_file(Import* self, char* path)
{
	auto csv = &self->csv;
	csv_reset(csv);

	// check path type
	Str path_str;
	str_set_cstr(&path_str, path);
	if (! str_is_postfix(&path_str, ".csv", 4))
		error("import: '{s}' csv file expected", path);

	// open and mmap file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_rdonly(&file, path);

	Mmap mmap;
	mmap_init(&mmap);
	mmap_file(&mmap, &file);
	defer(mmap_unmap, &mmap);

	csv_set(csv, &mmap.mmap);

	// read csv file in batches
	import_report(self, &file, 0);
	auto processed = 0ull;
	auto processed_report = 0ull;
	for (;;)
	{
		Str batch;
		str_init(&batch);
		auto rc = csv_collect(csv, &batch, opt_int_of(&self->batch));
		if (rc == CSV_EOF)
			break;

		import_send(self, &batch);

		// report
		processed += str_size(&batch);
		processed_report += str_size(&batch);
		if (processed_report >= 100 * 1024 * 1024)
		{
			import_report(self, &file, processed);
			processed_report = 0;
		}
	}

	// read the rest of replies
	import_sync_all(self);

	// report
	import_report(self, &file, processed);
	info("\n");
}

static void
import_main(Import* self)
{
	// ensure relation is defined
	if (opt_string_empty(&self->main->endpoint.target))
		error("import: target relation is not set\n");

	// set endpoint service as import
	opt_int_set(&self->main->endpoint.endpoint, ENDPOINT_IMPORT);

	// create clients and connect
	import_connect(self);
	self->report_time = time_us();

	// import files or stdin
	auto argc = self->main->argc;
	auto argv = self->main->argv;
	if (! argc)
		error("import: no files defined\n");
	while (argc > 0)
	{
		self->report_processed = 0;
		import_file(self, argv[0]);
		argc--;
		argv++;
	}
}

void
import_run(Import* self)
{
	// connect and import files
	error_catch( import_main(self) );

	// disconnect clients
	import_disconnect(self);
}
