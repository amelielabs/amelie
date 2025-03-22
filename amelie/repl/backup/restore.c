
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>

typedef struct Restore Restore;

struct Restore
{
	int64_t checkpoint;
	int64_t step;
	int64_t step_total;
	Client* client;
	Remote* remote;
	Buf     state;
};

static void
restore_init(Restore* self, Remote* remote)
{
	self->checkpoint  = 0;
	self->step        = 0;
	self->step_total  = 0;
	self->remote      = remote;
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
	client_set_remote(client, self->remote);
	client_connect(client);
}

static void
restore_start(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;
	auto token     = remote_get(self->remote, REMOTE_TOKEN);

	// begin backup

	// POST /
	auto request = &client->request;
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Am-Service", "backup");
	http_write(request, "Am-Version", "1");
	http_write_end(request);
	tcp_write_buf(tcp, &request->raw);

	// read backup state
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");
	http_read_content(reply, readahead, &reply->content);

	// Am-Service
	auto am_service = http_find(reply, "Am-Service", 10);
	if (unlikely(! am_service))
		error("backup Am-Service field is missing");
	if (unlikely(! str_is(&am_service->value, "backup", 6)))
		error("backup Am-Service is invalid");

	// Am-Version
	auto am_version = http_find(reply, "Am-Version", 10);
	if (unlikely(! am_version))
		error("backup Am-Version field is missing");
	if (unlikely(! str_is(&am_version->value, "1", 1)))
		error("backup Am-Version is invalid");

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

	// create checkpoints directory
	fs_mkdir(0755, "%s/checkpoints", state_directory());
	if (self->checkpoint > 0)
		fs_mkdir(0755, "%s/checkpoints/%" PRIi64, state_directory(),
		         self->checkpoint);

	// create wals directory
	fs_mkdir(0755, "%s/wals", state_directory());
}

static void
restore_next(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;
	auto token     = remote_get(self->remote, REMOTE_TOKEN);

	// request next step

	// POST /
	auto request = &client->request;
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Am-Service", "backup");
	http_write(request, "Am-Version", "1");
	http_write(request, "Am-Step", "%" PRIi64, self->step);
	http_write_end(request);
	tcp_write_buf(tcp, &request->raw);

	// read response
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");

	// Am-Service
	auto am_service = http_find(reply, "Am-Service", 10);
	if (unlikely(! am_service))
		error("backup Am-Service field is missing");
	if (unlikely(! str_is(&am_service->value, "backup", 6)))
		error("backup Am-Service is invalid");

	// Am-Version
	auto am_version = http_find(reply, "Am-Version", 10);
	if (unlikely(! am_version))
		error("backup Am-Version field is missing");
	if (unlikely(! str_is(&am_version->value, "1", 1)))
		error("backup Am-Version is invalid");

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
	auto content_len = http_find(reply, "Content-Length", 14);
	if (! content_len)
		error("response Content-Length is missing");
	int64_t len;
	if (str_toint(&content_len->value, &len) == -1)
		error("failed to parse Content-Length");

	// expect correct step
	if (step != self->step || step >= self->step_total )
		error("backup Am-Step order is invalid");

	info("[%3" PRIi64 "/%3" PRIi64 "] %.*s (%" PRIu64 " bytes)",
	     self->step + 1,
	     self->step_total,
	     str_size(&am_file->value),
	     str_of(&am_file->value),
	     len);

	// create and transfer file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", state_directory(),
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
restore(Remote* remote)
{
	coroutine_set_name(am_self(), "restore");

	Restore restore;
	restore_init(&restore, remote);
	defer(restore_free, &restore);
	restore_connect(&restore);
	restore_start(&restore);
	for (; restore.step < restore.step_total; restore.step++)
		restore_next(&restore);
	info("complete");
}
