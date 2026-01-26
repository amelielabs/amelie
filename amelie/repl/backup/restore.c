
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_backup.h>

#if 0
typedef struct Restore Restore;

struct Restore
{
	int64_t   checkpoint;
	int64_t   step;
	int64_t   step_total;
	Client*   client;
	Endpoint* endpoint;
	Buf       state;
};

static void
restore_init(Restore* self, Endpoint* endpoint)
{
	self->checkpoint  = 0;
	self->step        = 0;
	self->step_total  = 0;
	self->client      = NULL;
	self->endpoint    = endpoint;
	buf_init(&self->state);
}

static void
restore_free(Restore* self)
{
	if (self->client)
		client_free(self->client);
	buf_free(&self->state);
}

static void
restore_connect(Restore* self)
{
	self->client = client_create();
	auto client = self->client;
	client->readahead.readahead = 256 * 1024;
	client_set_endpoint(client, self->endpoint);
	client_connect(client);
}

static void
restore_create(char* directory)
{
	// ensure directory does not exists
	if (fs_exists("%s", directory))
		error("directory already exists");

	// set directory
	opt_string_set_raw(&state()->directory, directory, strlen(directory));

	// <base>/
	fs_mkdir(0755, "%s", state_directory());

	// <base>/certs
	fs_mkdir(0755, "%s/certs", state_directory());

	// <base>/checkpoints
	fs_mkdir(0755, "%s/checkpoints", state_directory());

	// <base>/wals
	fs_mkdir(0755, "%s/wals", state_directory());
}

static void
restore_start(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// begin backup

	// POST /v1/backup
	//
	// accept application/json
	//
	opt_string_set_raw(&client->endpoint->service, "backup", 6);

	auto request = &client->request;
	auto buf = http_begin_request(request, client->endpoint, 0);
	http_end(buf);
	tcp_write_buf(tcp, buf);

	// read backup state
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");
	http_read_content(reply, readahead, &reply->content);

	// parse state
	Str text;
	buf_str(&reply->content, &text);
	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &text, &self->state);

	uint8_t* pos = self->state.start;
	Decode obj[] =
	{
		{ DECODE_INT, "checkpoint", &self->checkpoint },
		{ DECODE_INT, "steps",      &self->step_total },
		{ 0,           NULL,        NULL              }
	};
	decode_obj(obj, "restore", &pos);

	// create checkpoint directory
	if (self->checkpoint > 0)
		fs_mkdir(0755, "%s/checkpoints/%" PRIi64, state_directory(),
		         self->checkpoint);
}

static void
restore_next(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// request next step

	// POST /v1/backup
	//
	// accept application/octet-stream
	auto request = &client->request;
	auto buf = http_begin_request(request, client->endpoint, 0);
	buf_printf(buf, "Am-Step: %" PRIu64 "\r\n", self->step);
	http_end(buf);
	tcp_write_buf(tcp, buf);

	// read response
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");

	// todo: validate endpoint /v1/backup

	// Am-Step
	auto am_step = http_find(reply, "Am-Step", 7);
	if (unlikely(! am_step))
		error("backup Am-Step field is missing");
	int64_t step;
	if (str_toint(&am_step->value, &step) == -1)
		error("backup Am-Step is invalid");

	// Am-File
	auto am_file = http_find(reply, "Am-File", 7);
	if (unlikely(! am_file))
		error("backup Am-File field is missing");

	// read content header

	// content-length might be missing for empty files
	int64_t len = 0;
	auto content_len = http_find(reply, "Content-Length", 14);
	if (content_len)
	{
		if (str_toint(&content_len->value, &len) == -1)
			error("failed to parse Content-Length");
	}

	// expect correct step
	if (step != self->step || step >= self->step_total )
		error("backup Am-Step order is invalid");

	info("%s/%.*s (%.2f MiB)", state_directory(),
	     str_size(&am_file->value),
	     str_of(&am_file->value),
	     (double)len / 1024 / 1024);

	// create and transfer file
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(&am_file->value),
	     str_of(&am_file->value));
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_EXCL|O_RDWR, 0644);
	while (len > 0)
	{
		int to_read = readahead->readahead;
		if (len < to_read)
			to_read = len;
		uint8_t* pos = NULL;
		int size = readahead_read(readahead, to_read, &pos);
		if (size == 0)
			error("unexpected eof");
		file_write(&file, pos, size);
		len -= size;
	}
}

void
restore(Endpoint* endpoint, char* directory)
{
	coroutine_set_name(am_self(), "restore");

	Restore restore;
	restore_init(&restore, endpoint);
	defer(restore_free, &restore);
	restore_create(directory);
	restore_connect(&restore);
	restore_start(&restore);
	for (; restore.step < restore.step_total; restore.step++)
		restore_next(&restore);
}
#endif

void
restore(Endpoint* endpoint, char* directory)
{
	(void)endpoint;
	(void)directory;
}
