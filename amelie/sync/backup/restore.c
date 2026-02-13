
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

typedef struct Restore Restore;

struct Restore
{
	Buf       data;
	Client*   client;
	Endpoint* endpoint;
};

static void
restore_init(Restore* self, Endpoint* endpoint)
{
	self->client   = NULL;
	self->endpoint = endpoint;
	buf_init(&self->data);
}

static void
restore_free(Restore* self)
{
	if (self->client)
		client_free(self->client);
	buf_free(&self->data);
}

static void
restore_dir_str(Str* path_relative)
{
	// <base>/<path_relative>
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(path_relative),
	     str_of(path_relative));

	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	info("backup: %.*s", str_size(path_relative),
	     str_of(path_relative));
}

static void
restore_dir(char* path_relative)
{
	Str str;
	str_set_cstr(&str, path_relative);
	restore_dir_str(&str);
}

static void
restore_basedir(char* directory)
{
	// ensure directory does not exists
	if (fs_exists("%s", directory))
		error("directory already exists");

	// set directory
	opt_string_set_raw(&state()->directory, directory, strlen(directory));

	// <base>/
	restore_dir("");

	// <base>/certs
	restore_dir("certs");

	// <base>/storage
	restore_dir("storage");

	// <base>/wal
	restore_dir("wal");
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
restore_snapshot(Restore* self)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// POST /v1/backup (application/octet-stream)
	opt_string_set_raw(&client->endpoint->service, "backup", 6);

	auto request = &client->request;
	auto buf = http_begin_request(request, client->endpoint, 0);
	http_end(buf);
	tcp_write_buf(tcp, buf);

	// read snapshot data
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");
	http_read_content(reply, readahead, &self->data);
}

static void
restore_file(char* path_relative, uint8_t* data)
{
	// <base>/<path_relative>
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%s", state_directory(), path_relative);

	// convert to json
	auto buf = buf_create();
	defer_buf(buf);
	json_export_pretty(buf, NULL, &data);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, buf);

	info("backup: %s", path_relative);
}

static void
restore_file_remote(Restore* self, Str* path_relative, int64_t size, int mode)
{
	auto client    = self->client;
	auto tcp       = &client->tcp;
	auto readahead = &client->readahead;

	// POST /v1/backup
	//
	// accept application/octet-stream
	auto request = &client->request;
	auto buf = http_begin_request(request, client->endpoint, 0);
	buf_printf(buf, "Am-File: %.*s" "\r\n", str_size(path_relative),
	           str_of(path_relative));
	buf_printf(buf, "Am-FileSize: %" PRIu64 "\r\n", size);
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

	// cut of the .snapshot extensions
	if (str_size(path_relative) > 9 && !strncmp(path_relative->end - 9, ".snapshot", 9))
		str_truncate(path_relative, 9);

	// create and transfer the file
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(path_relative),
	     str_of(path_relative));


	info("backup: %.*s (%.2f MiB)", str_size(path_relative),
	     str_of(path_relative),
	     (double)len / 1024 / 1024);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_EXCL|O_RDWR, mode);
	while (len > 0)
	{
		int to_read = readahead->readahead;
		if (len < to_read)
			to_read = len;
		uint8_t* pos = NULL;
		auto rc = readahead_read(readahead, to_read, &pos);
		if (rc == 0)
			error("unexpected eof");
		file_write(&file, pos, rc);
		len -= rc;
	}
}

static void
restore_run(Restore* self, char* directory)
{
	// connect to the remote server
	restore_connect(self);

	// create base directory
	restore_basedir(directory);

	// create and read remote snapshot data
	restore_snapshot(self);

	// parse data
	auto pos = self->data.start;
	uint8_t* pos_version    = NULL;
	uint8_t* pos_config     = NULL;
	uint8_t* pos_state      = NULL;
	uint8_t* pos_catalog    = NULL;
	uint8_t* pos_storage    = NULL;
	uint8_t* pos_partitions = NULL;
	uint8_t* pos_wal        = NULL;
	Decode obj[] =
	{
		{ DECODE_OBJ,   "version",    &pos_version    },
		{ DECODE_OBJ,   "config",     &pos_config     },
		{ DECODE_OBJ,   "state",      &pos_state      },
		{ DECODE_OBJ,   "catalog",    &pos_catalog    },
		{ DECODE_ARRAY, "storage",    &pos_storage    },
		{ DECODE_ARRAY, "partitions", &pos_partitions },
		{ DECODE_ARRAY, "wal",        &pos_wal        },
		{ 0,             NULL,        NULL            },
	};
	decode_obj(obj, "snapshot", &pos);

	// write version
	restore_file("version.json", pos_version);

	// write config
	restore_file("config.json", pos_config);

	// write state
	restore_file("state.json", pos_state);

	// write catalog
	restore_file("catalog.json", pos_catalog);

	// create tier storage directories
	json_read_array(&pos_storage);
	while (! json_read_array_end(&pos_storage))
	{
		Str path_relative;
		json_read_string(&pos_storage, &path_relative);
		restore_dir_str(&path_relative);
	}

	// fetch partitions files
	json_read_array(&pos_partitions);
	while (! json_read_array_end(&pos_partitions))
	{
		// [path_relative, size, mode]
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_partitions, &path_relative, &size, &mode);
		restore_file_remote(self, &path_relative, size, mode);
	}

	// fetch wal files
	json_read_array(&pos_wal);
	while (! json_read_array_end(&pos_wal))
	{
		// [path_relative, size, mode]
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_wal, &path_relative, &size, &mode);
		restore_file_remote(self, &path_relative, size, mode);
	}
}

void
restore(Endpoint* endpoint, char* directory)
{
	coroutine_set_name(am_self(), "restore");

	Restore restore;
	restore_init(&restore, endpoint);
	defer(restore_free, &restore);
	restore_run(&restore, directory);
}
