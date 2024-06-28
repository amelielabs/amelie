
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_planner.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>

typedef struct Restore Restore;

struct Restore
{
	int64_t  checkpoint;
	uint8_t* files;
	uint8_t* config;
	Client*  client;
	Buf      buf;
	Buf      state_data;
	Buf      state;
};

static void
restore_init(Restore* self)
{
	self->checkpoint = 0;
	self->files      = NULL;
	self->config     = NULL;
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
restore_connect(Restore* self, Str* uri)
{
	self->client = client_create();
	auto client = self->client;
	client->readahead.readahead = 256 * 1024;
	client_set_uri(client, uri);
	client_connect(client);
}

static void
restore_start(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// GET /backup
	auto request = &client->request;
	http_write_request(request, "GET /backup");
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
	Decode map[] =
	{
		{ DECODE_INT,   "checkpoint", &self->checkpoint },
		{ DECODE_ARRAY, "files",      &self->files      },
		{ DECODE_MAP,   "config",     &self->config     },
		{ 0,             NULL,        NULL              }
	};
	decode_map(map, &pos);

	// create checkpoint directory
	if (self->checkpoint > 0)
		fs_mkdir(0755, "%s/%" PRIi64, config_directory(),
		         self->checkpoint);

	// create wal directory
	fs_mkdir(0755, "%s/wal", config_directory());
}

static void
restore_copy_file(Restore* self, Str* name)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// GET /backup/<path>
	auto request = &client->request;
	http_write_request(request, "GET /backup/%.*s", str_size(name),
	                   str_of(name));
	http_write_end(request);
	tcp_write_buf(tcp, &request->raw);

	// read response
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");

	// read content header
	auto content_len = http_find(reply, "content-length", 14);
	if (! content_len)
		error("response content-length is missing");
	int64_t len;
	if (str_toint(&content_len->value, &len) == -1)
		error("failed to parse content-length");

	// create file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", config_directory(),
	         str_size(name), str_of(name));

	log("%.*s (%" PRIu64 " bytes)", str_size(name),
	    str_of(name), len);

	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);

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
		Str name;
		data_read_string(&pos, &name);
		restore_copy_file(self, &name);
	}
}

static void
restore_write_config(Restore* self)
{
	// convert config to json
	Buf text;
	buf_init(&text);
	guard(buf_free, &text);
	uint8_t* pos = self->config;
	json_export_pretty(&text, &pos);

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
restore(Str* uri)
{
	coroutine_set_name(so_self(), "restore");

	Restore restore;
	restore_init(&restore);
	guard(restore_free, &restore);
	restore_connect(&restore, uri);
	restore_start(&restore);
	restore_copy(&restore);
	restore_write_config(&restore);

	log("complete.");
}
