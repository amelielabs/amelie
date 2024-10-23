
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_private.h>

static ImportClient*
import_client_allocate(void)
{
	auto self = (ImportClient*)am_malloc(sizeof(ImportClient));
	self->client  = client_create();
	self->pending = false;
	list_init(&self->link);
	return self;
}

static void
import_client_free(ImportClient* self)
{
	client_free(self->client);
	am_free(self);
}

static inline void
import_client_send(ImportClient* self, Str* path, Str* content_type, Str* content)
{
	auto client  = self->client;
	auto request = &client->request;

	// POST /schema/table
	http_write_request(request, "POST %.*s", str_size(path), str_of(path));
	auto token = remote_get(client->remote, REMOTE_TOKEN);
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "%.*s", str_size(content_type), str_of(content_type));
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&client->tcp, &request->raw, content);

	self->pending++;
}

static inline void
import_client_recv(ImportClient* self, Import* import)
{
	auto client = self->client;
	auto reply  = &client->reply;
	if (self->pending == 0)
		return;

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);

	self->pending--;
	if (unlikely(str_is_prefix(&reply->options[HTTP_CODE], "4", 1)))
		import->errors++;

	/*
	if (unlikely(str_is_prefix(&reply->options[HTTP_CODE], "4", 1)))
	{
		auto code = &reply->options[HTTP_CODE];
		auto msg  = &reply->options[HTTP_MSG];
		info("%.*s %.*s", str_size(code), str_of(code),
		     str_size(msg), str_of(msg));
		// todo: show content
	}
	*/
}

void
import_init(Import* self, Remote* remote)
{
	self->rows    = 0;
	self->errors  = 0;
	self->remote  = remote;
	self->forward = NULL;
	list_init(&self->clients_list);

	reader_init(&self->reader);
	vars_init(&self->vars);
	VarDef defs[] =
	{
		{ "format",  VAR_STRING, VAR_C, &self->format,  "auto", 0   },
		{ "batch",   VAR_INT,    VAR_C, &self->batch,    NULL,  500 },
		{ "clients", VAR_INT,    VAR_C, &self->clients,  NULL,  12  },
		{  NULL,     0,          0,     NULL,            NULL,  0   }
	};
	vars_define(&self->vars, defs);
}

void
import_free(Import* self)
{
	reader_free(&self->reader);
	vars_free(&self->vars);
}

static bool
import_path_read(Str* target, Str* schema, Str* name)
{
	// [schema.]name
	if (str_empty(target))
		return false;

	auto pos = target->pos;
	auto end = target->end;

	// [schema.] or name
	auto start = pos;
	while (pos < end && *pos != '.')
	{
		if (unlikely(! isalnum(*pos)))
			return false;
		pos++;
	}

	// set as name using public schema
	if (pos == end)
	{
		str_set_cstr(schema, "public");
		str_set(name, start, pos - start);
		if (unlikely(str_empty(name)))
			return false;
		return true;
	}

	// schema
	str_set(schema, start, pos - start);
	if (unlikely(str_empty(schema)))
		return false;

	// .
	pos++;

	// name
	start = pos;
	while (pos < end)
	{
		if (unlikely(! isalnum(*pos)))
			return false;
		pos++;
	}

	str_set(name, start, pos - start);
	if (unlikely(str_empty(name)))
		return false;

	return true;
}

static Buf*
import_path_create(Import* self, char* target_path)
{
	Str target;
	str_set_cstr(&target, target_path);

	// [schema.]name
	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);
	if (! import_path_read(&target, &schema, &name))
		return NULL;

	// /schema/name
	auto path = buf_create();
	buf_write(path, "/", 1);
	buf_write_str(path, &schema);
	buf_write(path, "/", 1);
	buf_write_str(path, &name);
	buf_str(path, &self->path);
	return path;
}

static void
import_set_format(Import* self, int argc, char** argv)
{
	// set format
	auto format = var_string_of(&self->format);
	if (argc == 0)
	{
		// stdin
		if (str_is_cstr(format, "auto"))
			error("format argument is required");
	} else
	{
		// guess format based on the file extensions
		if (str_is_cstr(format, "auto"))
		{
			for (int i = 0; i < argc; i++)
			{
				Str guess;
				str_init(&guess);

				if (strstr(argv[i], ".jsonl"))
					str_set_cstr(&guess, "jsonl");
				else
				if (strstr(argv[i], ".json"))
					str_set_cstr(&guess, "json");
				else
				if (strstr(argv[i], ".csv"))
					str_set_cstr(&guess, "csv");
				else
					error("format argument is required");

				// ensure files have the same format
				if (str_is_cstr(format, "auto")) {
					var_string_set(&self->format, &guess);
				} else {
					if (! str_compare(format, &guess))
						error("imported files must share the same format");
				}
			}
		}
	}

	// set content type
	if (str_is_cstr(format, "jsonl"))
		str_set_cstr(&self->content_type, "application/jsonl");
	else
	if (str_is_cstr(format, "csv"))
		str_set_cstr(&self->content_type, "text/csv");
	else
		error("unknown import format '%.*s'", str_size(format),
		      str_of(format));
}

static void
import_connect(Import* self)
{
	// create clients and connect
	int count = var_int_of(&self->clients);
	while (count-- > 0)
	{
		auto client = import_client_allocate();
		list_append(&self->clients_list, &client->link);
		client_set_remote(client->client, self->remote);
		client_connect(client->client);
	}
}

static void
import_disconnect(Import* self)
{
	list_foreach_safe(&self->clients_list)
	{
		auto client = list_at(ImportClient, link);
		client_close(client->client);
		import_client_free(client);
	}
}

static void
import_send(Import* self, Buf* buf)
{
	ImportClient* next;
	if (!self->forward || list_is_last(&self->clients_list, &self->forward->link))
	{
		auto first = list_first(&self->clients_list);
		next = container_of(first, ImportClient, link);
	} else {
		next = container_of(self->forward->link.next, ImportClient, link);
	}

	// read reply from previous request
	import_client_recv(next, self);

	// POST /schema/table
	Str content;
	buf_str(buf, &content);
	import_client_send(next, &self->path, &self->content_type, &content);

	self->forward = next;
}

static void
import_sync(Import* self)
{
	list_foreach(&self->clients_list)
	{
		auto client = list_at(ImportClient, link);
		while (client->pending > 0)
			import_client_recv(client, self);
	}
}

hot static inline void
import_report(Import* self, char* path)
{
	auto reader = &self->reader;
	if (path)
	{
		auto total_mib = reader->file.size / 1024 / 1024;
		auto done_mib  = reader->offset_file / 1024 / 1024;
		int  percent   = 0;
		if (total_mib > 0)
			percent = (done_mib * 100ull) / total_mib;
		else
		if (total_mib == 0)
			percent = 100;

		printf("%.*s %d%% (%d MiB / %d MiB) %" PRIu64 " rows, %" PRIu64 " errors\r",
		       str_size(&reader->file.path),
		       str_of(&reader->file.path),
		       (int)percent, (int)done_mib, (int)total_mib,
		       self->rows,
		       self->errors);
	} else
	{
		auto done_mib = reader->offset_file / 1024 / 1024;
		printf("stdin (%d MiB) %" PRIu64 " rows, %" PRIu64 " errors\r",
		       (int)done_mib, self->rows, self->errors);
	}

	fflush(stdout);
}

static void
import_file(Import* self, char* path)
{
	int limit = var_int_of(&self->batch);
	int limit_size = 256 * 1024;

	auto reader = &self->reader;
	reader_reset(reader);
	reader_open(reader, READER_LINE, path, limit, limit_size);

	// report
	auto done = reader->offset_file;
	import_report(self, path);
	for (;;)
	{
		int  rows = 0;
		auto buf = reader_read(reader, &rows);
		if (! buf)
			break;
		guard_buf(buf);

		import_send(self, buf);
		self->rows += rows;

		// report
		if ((reader->offset_file - done) >= 10 * 1024 * 1024)
		{
			import_report(self, path);
			done = reader->offset_file;
		}
	}

	// read the rest of replies
	import_sync(self);

	// report
	import_report(self, path);
	printf("\n");

	reader_close(reader);
}

void
import_run(Import* self, int argc, char** argv)
{
	// validate clients and batch size
	if (!var_int_of(&self->clients) ||
	    !var_int_of(&self->batch))
		error("clients and batch arguments cannot be set to zero");

	// read table name and set path
	if (argc == 0)
		error("table name expected");
	auto path = import_path_create(self, argv[0]);
	guard_buf(path);
	buf_str(path, &self->path);
	argc--;
	argv++;

	// guess format and set content type
	import_set_format(self, argc, argv);

	Exception e;
	if (enter(&e))
	{
		// create clients and connect
		import_connect(self);

		// import files or stdin
		if (argc > 0)
		{
			while (argc > 0)
			{
				import_file(self, argv[0]);
				argc--;
				argv++;
			}
		} else {
			import_file(self, NULL);
		}
	}

	if (leave(&e))
	{ }

	// disconnect clients
	import_disconnect(self);
}
