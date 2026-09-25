
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
#include <amelie_main_copy.h>

void
copy_init(Copy* self, Main* main)
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
copy_free(Copy* self)
{
	csv_free(&self->csv);
	opts_free(&self->opts);
}

static void
copy_connect(Copy* self)
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
copy_disconnect(Copy* self)
{
	list_foreach_safe(&self->clients_list)
	{
		auto client = list_at(Client, link);
		client_free(client);
	}
}

static void
copy_sync(Copy* self, Client* client)
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
copy_sync_all(Copy* self)
{
	list_foreach(&self->clients_list)
	{
		auto client = list_at(Client, link);
		copy_sync(self, client);
	}
}

static void
copy_send(Copy* self, Str* content)
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
	copy_sync(self, next);

	// POST /?copy
	client_send(next, content);
	next->sync++;

	self->forward = next;
}

hot static inline void
copy_report(Copy* self, File* file, uint64_t processed)
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
copy_file(Copy* self, char* path)
{
	auto csv = &self->csv;
	csv_reset(csv);

	// check path type
	Str path_str;
	str_set_cstr(&path_str, path);
	if (! str_is_postfix(&path_str, ".csv", 4))
		error("copy: '{s}' csv file expected", path);

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
	copy_report(self, &file, 0);
	auto processed = 0ull;
	auto processed_report = 0ull;
	for (;;)
	{
		Str batch;
		str_init(&batch);
		auto rc = csv_collect(csv, &batch, opt_int_of(&self->batch));
		if (rc == CSV_EOF)
			break;

		copy_send(self, &batch);

		// report
		processed += str_size(&batch);
		processed_report += str_size(&batch);
		if (processed_report >= 100 * 1024 * 1024)
		{
			copy_report(self, &file, processed);
			processed_report = 0;
		}
	}

	// read the rest of replies
	copy_sync_all(self);

	// report
	copy_report(self, &file, processed);
	info("\n");
}

static void
copy_main(Copy* self)
{
	// create clients and connect
	copy_connect(self);
	self->report_time = time_us();

	// copy files or stdin
	auto argc = self->main->argc;
	auto argv = self->main->argv;
	if (! argc)
		error("copy: no files defined\n");
	while (argc > 0)
	{
		self->report_processed = 0;
		copy_file(self, argv[0]);
		argc--;
		argv++;
	}
}

void
copy_run(Copy* self)
{
	// connect and copy files
	error_catch( copy_main(self) );

	// disconnect clients
	copy_disconnect(self);
}
