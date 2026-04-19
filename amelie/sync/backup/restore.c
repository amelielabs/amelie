
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
	Websocket websocket;
	Client*   client;
	Endpoint* endpoint;
};

static void
restore_init(Restore* self, Endpoint* endpoint)
{
	self->client   = NULL;
	self->endpoint = endpoint;
	buf_init(&self->data);
	websocket_init(&self->websocket);
}

static void
restore_free(Restore* self)
{
	if (self->client)
		client_free(self->client);
	websocket_free(&self->websocket);
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

	// set websocket
	Str protocol;
	str_set(&protocol, "amelie-v1-backup", 16);
	websocket_set(&self->websocket, &protocol, client, true);
}

static void
restore_join(Restore* self)
{
	// POST /v1/backup
	auto websocket = &self->websocket;
	opt_int_set(&websocket->client->endpoint->endpoint, ENDPOINT_BACKUP);

	// do websocket handshake
	websocket_connect(websocket);

	// BACKUP_JOIN

	// read snapshot data
	auto op = backup_recv(websocket, &self->data);
	if (op != BACKUP_JOIN)
		error("restore: unexpected server response");
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
restore_pull(Restore* self, Str* path_relative, int64_t size, int mode)
{
	// BACKUP_PULL

	// [file, size]
	auto websocket = &self->websocket;
	auto client = self->client;
	auto buf = &client->request.content;
	buf_reset(buf);
	encode_array(buf);
	encode_string(buf, path_relative);
	encode_integer(buf, size);
	encode_array_end(buf);
	backup_send(websocket, BACKUP_PULL, buf, 0);

	// recv BACKUP_PUSH
	buf_reset(buf);
	auto op = backup_recv(websocket, buf);
	if (op != BACKUP_PUSH)
		error("restore: unexpected server response");

	// cut of the .snapshot extensions
	if (str_size(path_relative) > 9 && !strncmp(path_relative->end - 9, ".snapshot", 9))
		str_truncate(path_relative, 9);

	info("backup: %.*s (%.2f MiB)", str_size(path_relative),
	     str_of(path_relative),
	     (double)size / 1024 / 1024);

	// create and transfer the file
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(path_relative),
	     str_of(path_relative));

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_EXCL|O_RDWR, mode);
	while (size > 0)
	{
		auto to_read = client->readahead.readahead;
		if (size < to_read)
			to_read = size;
		uint8_t* pos = NULL;
		auto rc = readahead_read(&client->readahead, to_read, &pos);
		if (rc == 0)
			error("unexpected eof");
		file_write(&file, pos, rc);
		size -= rc;
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
	restore_join(self);

	// parse data
	auto pos = self->data.start;
	uint8_t* pos_version = NULL;
	uint8_t* pos_config  = NULL;
	uint8_t* pos_state   = NULL;
	uint8_t* pos_catalog = NULL;
	uint8_t* pos_volumes = NULL;
	uint8_t* pos_files   = NULL;
	uint8_t* pos_wal     = NULL;
	Decode obj[] =
	{
		{ DECODE_OBJ,   "version", &pos_version },
		{ DECODE_OBJ,   "config",  &pos_config  },
		{ DECODE_OBJ,   "state",   &pos_state   },
		{ DECODE_OBJ,   "catalog", &pos_catalog },
		{ DECODE_ARRAY, "volumes", &pos_volumes },
		{ DECODE_ARRAY, "files",   &pos_files   },
		{ DECODE_ARRAY, "wal",     &pos_wal     },
		{ 0,             NULL,      NULL        },
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

	// create volumes
	json_read_array(&pos_volumes);
	while (! json_read_array_end(&pos_volumes))
	{
		Str path_relative;
		json_read_string(&pos_volumes, &path_relative);
		restore_dir_str(&path_relative);
	}

	// pull files
	json_read_array(&pos_files);
	while (! json_read_array_end(&pos_files))
	{
		// [path_relative, size, mode]
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_files, &path_relative, &size, &mode);
		restore_pull(self, &path_relative, size, mode);
	}

	// pull wal files
	json_read_array(&pos_wal);
	while (! json_read_array_end(&pos_wal))
	{
		// [path_relative, size, mode]
		Str     path_relative;
		int64_t size;
		int64_t mode;
		decode_basefile(&pos_wal, &path_relative, &size, &mode);
		restore_pull(self, &path_relative, size, mode);
	}

	// BACKUP_DONE
	backup_send(&self->websocket, BACKUP_DONE, NULL, 0);
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
