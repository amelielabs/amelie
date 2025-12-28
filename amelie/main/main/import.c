
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

void
import_init(Import* self, Main* main)
{
	self->rows             = 0;
	self->errors           = 0;
	self->report_time      = 0;
	self->report_processed = 0;
	self->report_rows      = 0;
	self->main             = main;
	self->forward          = NULL;
	list_init(&self->clients_list);

	load_init(&self->load);
	opts_init(&self->opts);
	OptsDef defs[] =
	{
		{ "batch",   OPT_INT, OPT_C|OPT_Z, &self->batch,   NULL,  500 },
		{ "clients", OPT_INT, OPT_C|OPT_Z, &self->clients, NULL,  12  },
		{  NULL,     0,       0,            NULL,          NULL,  0   }
	};
	opts_define(&self->opts, defs);
}

void
import_free(Import* self)
{
	load_free(&self->load);
	opts_free(&self->opts);
}

static void
import_connect(Import* self)
{
	// create clients and connect
	int count = opt_int_of(&self->clients);
	while (count-- > 0)
	{
		auto client = main_client_create(self->main);
		list_append(&self->clients_list, &client->link);
		main_client_connect(client);
	}
}

static void
import_disconnect(Import* self)
{
	list_foreach_safe(&self->clients_list)
	{
		auto client = list_at(MainClient, link);
		main_client_free(client);
	}
}

static void
import_sync(Import* self, MainClient* client)
{
	while (client->pending > 0)
	{
		auto code = client->iface->recv(client, NULL);
		if (code != 200 && code != 204)
			self->errors++;
	}
}

static void
import_sync_all(Import* self)
{
	list_foreach(&self->clients_list)
	{
		auto client = list_at(MainClient, link);
		import_sync(self, client);
	}
}

static void
import_send(Import* self, Buf* buf)
{
	MainClient* next;
	if (!self->forward || list_is_last(&self->clients_list, &self->forward->link))
	{
		auto first = list_first(&self->clients_list);
		next = container_of(first, MainClient, link);
	} else {
		next = container_of(self->forward->link.next, MainClient, link);
	}

	// read reply from previous request
	import_sync(self, next);

	// POST /v1/db/db_name/relation
	Str content;
	buf_str(buf, &content);
	next->iface->send(next, &content);

	self->forward = next;
}

hot static inline void
import_report(Import* self, char* path)
{
	auto load = &self->load;
	timer_mgr_reset(&am_task->timer_mgr);

	auto     time           = time_us();
	double   time_diff      = (time - self->report_time) / 1000.0 / 1000.0;
	uint64_t processed_diff = load->offset_file - self->report_processed;
	uint64_t rows_diff      = self->rows - self->report_rows;
	int      processed_sec  = 0;
	int      rows_sec       = 0;
	if (time_diff > 0)
	{
		processed_sec = (int)(((double)processed_diff / 1024 / 1024) / time_diff);
		rows_sec      = (int)(((double)rows_diff) / time_diff);
	}

	if (path)
	{
		auto total_mib = load->file.size / 1024 / 1024;
		auto done_mib  = load->offset_file / 1024 / 1024;
		int  percent   = 0;
		if (total_mib > 0)
			percent = (done_mib * 100ull) / total_mib;
		else
		if (total_mib == 0)
			percent = 100;

		info("%.*s %d%% (%d MiB / %d MiB) %d MiB/sec, %d rows/sec, %" PRIu64 " errors\r",
		     str_size(&load->file.path),
		     str_of(&load->file.path),
		     (int)percent, (int)done_mib, (int)total_mib,
		     processed_sec,
		     rows_sec,
		     self->errors);
	} else
	{
		auto done_mib = load->offset_file / 1024 / 1024;
		info("pipe (%d MiB) %d MiB/sec, %d rows/sec, %" PRIu64 " errors\r",
		     (int)done_mib,
		     processed_sec,
		     rows_sec,
		     self->errors);
	}

	self->report_time      = time;
	self->report_processed = load->offset_file;
	self->report_rows      = self->rows;

	fflush(stdout);
}

static void
import_file(Import* self, char* path)
{
	int limit = opt_int_of(&self->batch);
	int limit_size = 256 * 1024;

	auto load = &self->load;
	load_reset(load);
	load_open(load, LOAD_LINE, path, limit, limit_size);

	// report
	auto done = load->offset_file;
	import_report(self, path);
	for (;;)
	{
		int  rows = 0;
		auto buf = load_read(load, &rows);
		if (! buf)
			break;
		defer_buf(buf);

		import_send(self, buf);
		self->rows += rows;

		// report
		if ((load->offset_file - done) >= 100 * 1024 * 1024)
		{
			import_report(self, path);
			done = load->offset_file;
		}
	}

	// read the rest of replies
	import_sync_all(self);

	// report
	import_report(self, path);
	info("\n");

	load_close(load);
}

static void
import_set_content_type(Import* self)
{
	auto endpoint = &self->main->endpoint;
	if (! opt_string_empty(&endpoint->content_type))
		return;

	auto argc = self->main->argc;
	auto argv = self->main->argv;
	if (! argc)
		error("import: content-type is not set\n");

	Str content_type;
	str_init(&content_type);
	for (auto i = 0; i < argc; i++)
	{
		Str file_type;
		str_init(&file_type);
		auto file = argv[i];
		if (strstr(file, ".jsonl"))
			str_set_cstr(&file_type, "application/jsonl");
		else
		if (strstr(file, ".json"))
			str_set_cstr(&file_type, "application/json");
		else
		if (strstr(file, ".csv"))
			str_set_cstr(&file_type, "text/csv");
		else
			error("import: unrecognized file '%s' content-type\n", file);

		if (str_empty(&content_type))
			content_type = file_type;
		else
		if (! str_compare(&content_type, &file_type))
			error("import: file types must match\n");
	}

	opt_string_set(&endpoint->content_type, &content_type);
}

static void
import_main(Import* self)
{
	// ensure relation is defined
	if (opt_string_empty(&self->main->endpoint.relation))
		error("import: target relation is not set\n");

	// set endpoint content type
	import_set_content_type(self);

	// create clients and connect
	import_connect(self);
	self->report_time = time_us();

	// import files or stdin
	auto argc = self->main->argc;
	auto argv = self->main->argv;
	if (argc > 0)
	{
		while (argc > 0)
		{
			self->report_processed = 0;
			import_file(self, argv[0]);
			argc--;
			argv++;
		}
	} else {
		import_file(self, NULL);
	}
}

void
import_run(Import* self)
{
	//connect and import files
	error_catch(
		import_main(self);
	);

	// disconnect clients
	import_disconnect(self);
}
