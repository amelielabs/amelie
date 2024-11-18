
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
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>

typedef struct Restore Restore;

struct Restore
{
	int64_t  checkpoint;
	uint8_t* files;
	uint8_t* config;
	int      count_total;
	int      count;
	Client*  client;
	Remote*  remote;
	Buf      buf;
	Buf      state_data;
	Buf      state;
};

static void
restore_init(Restore* self, Remote* remote)
{
	self->checkpoint  = 0;
	self->files       = NULL;
	self->config      = NULL;
	self->count       = 0;
	self->count_total = 0;
	self->remote      = remote;
	buf_init(&self->state_data);
	buf_init(&self->state);
	buf_init(&self->buf);
}

static void
restore_free(Restore* self)
{
	if (self->client)
		client_free(self->client);
	buf_free(&self->state_data);
	buf_free(&self->state);
	buf_free(&self->buf);
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

	// POST /
	auto request = &client->request;
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Am-Service", "backup");
	http_write_end(request);
	tcp_write_buf(tcp, &request->raw);

	// read backup state
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, readahead, &self->state);

	// parse state
	Str text;
	buf_str(&self->state, &text);
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &text, &self->state_data);

	uint8_t* pos = self->state_data.start;
	Decode obj[] =
	{
		{ DECODE_INT,   "checkpoint", &self->checkpoint },
		{ DECODE_ARRAY, "files",      &self->files      },
		{ DECODE_OBJ,   "config",     &self->config     },
		{ 0,             NULL,        NULL              }
	};
	decode_obj(obj, "restore", &pos);

	self->count_total = array_size(self->files);

	// create checkpoint directory
	if (self->checkpoint > 0)
		fs_mkdir(0755, "%s/%" PRIi64, config_directory(),
		         self->checkpoint);

	// create wal directory
	fs_mkdir(0755, "%s/wal", config_directory());
}

static void
restore_copy_file(Restore* self, Str* name, uint64_t size)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;
	auto token     = remote_get(self->remote, REMOTE_TOKEN);

	// POST /
	auto request = &client->request;
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Am-Service", "backup");
	http_write(request, "Am-File", "%.*s", str_size(name), str_of(name));
	http_write(request, "Am-FIle-Size", "%" PRIu64, size);
	http_write_end(request);
	tcp_write_buf(tcp, &request->raw);

	// read response
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");

	// read content header
	auto content_len = http_find(reply, "Content-Length", 14);
	if (! content_len)
		error("response Content-Length is missing");
	int64_t len;
	if (str_toint(&content_len->value, &len) == -1)
		error("failed to parse Content-Length");
	if (size != (uint64_t)len)
		error("response Content-Length is invalid (size mismatch)");

	// create file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", config_directory(),
	         str_size(name), str_of(name));

	self->count++;
	info("[%3d/%3d] %.*s (%" PRIu64 " bytes)",
	     self->count,
	     self->count_total,
	     str_size(name),
	     str_of(name), len);

	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_EXCL|O_RDWR, 0644);

	// read content into file
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

static void
restore_copy(Restore* self)
{
	uint8_t* pos = self->files;
	data_read_array(&pos);
	while (! data_read_array_end(&pos))
	{
		// [path, size]
		data_read_array(&pos);
		Str name;
		data_read_string(&pos, &name);
		int64_t size;
		data_read_integer(&pos, &size);
		data_read_array_end(&pos);
		restore_copy_file(self, &name, size);
	}
}

static void
restore_write_config(Restore* self)
{
	// convert config to json
	Buf text;
	buf_init(&text);
	guard_buf(&text);
	uint8_t* pos = self->config;
	json_export_pretty(&text, global()->timezone, &pos);

	// create config file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json",
	         config_directory());

	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);
}

void
restore(Remote* remote)
{
	coroutine_set_name(am_self(), "restore");

	Restore restore;
	restore_init(&restore, remote);
	guard(restore_free, &restore);
	restore_connect(&restore);
	restore_start(&restore);
	restore_copy(&restore);
	restore_write_config(&restore);

	info("complete");
}
