
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

void
backup_init(Backup* self, Db* db)
{
	self->snapshot = NULL;
	self->client   = NULL;
	self->db       = db;
	event_init(&self->on_complete);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	if (self->snapshot)
	{
		db_snapshot_drop(self->db, self->snapshot);
		self->snapshot = NULL;
	}
	task_free(&self->task);
}

static void
backup_send_snapshot(Backup* self)
{
	auto client = self->client;
	auto reply  = &client->reply;
	auto data   = &self->snapshot->data;

	// application/octet-stream
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6,
	                            buf_size(data));
	http_end(buf);
	tcp_write_pair(&client->tcp, buf, data);
}

static void
backup_send_file(Backup* self, Str* name, size_t size)
{
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(name), str_of(name));

	// prepare and send header
	auto client = self->client;
	auto reply  = &client->reply;
	auto tcp    = &client->tcp;

	// reply application/octet-stream
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6, size);
	buf_printf(buf, "Am-File: %.*s\r\n", str_size(name), str_of(name));
	http_end(buf);
	tcp_write_buf(tcp, buf);

	// transfer file content
	buf = &reply->content;
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);
	if (size > file.size)
		error("backup: requested file '%.*s'' size mismatch", str_size(name),
		      str_of(name));

	while (size > 0)
	{
		buf_reset(buf);
		size_t chunk = 256 * 1024;
		if (size < chunk)
			chunk = size;
		file_read_buf(&file, buf, chunk);
		tcp_write_buf(tcp, buf);
		buf_reset(buf);
		size -= chunk;
	}
}

static void
backup_send(Backup* self)
{
	auto client = self->client;
	auto request = &client->request;

	// Am-File
	auto am_file = http_find(request, "Am-File", 7);
	if (unlikely(! am_file))
		error("backup: Am-File field is missing");

	// Am-FileSize
	auto am_filesize = http_find(request, "Am-FileSize", 11);
	if (unlikely(! am_filesize))
		error("backup: Am-FileSize field is missing");

	// todo: validate file path

	int64_t size;
	if (str_toint(&am_filesize->value, &size) == -1)
		error("backup: Am-FileSize is invalid");

	info("backup: %.*s (%.2f MiB)", str_size(&am_file->value), str_of(&am_file->value),
	     (double)size / 1024 / 1024);

	// transmit file
	backup_send_file(self, &am_file->value, size);
}

static inline void
backup_process(Backup* self, Client* client)
{
	// create db snapshot
	self->snapshot = db_snapshot(self->db);

	// create and send snapshot data
	backup_send_snapshot(self);

	// process file requests (till disconnect)
	auto request = &client->request;
	auto readahead = &client->readahead;
	for (;;)
	{
		http_reset(request);
		auto eof = http_read(request, readahead, true);
		if (eof)
			break;
		http_read_content(request, readahead, &request->content);
		backup_send(self);
	}
}

static void
backup_main(void* arg)
{
	Backup* self   = arg;
	auto    client = self->client;
	auto    tcp    = &client->tcp;

	char addr[128];
	tcp_getpeername(&client->tcp, addr, sizeof(addr));

	info("");
	info("backup: sending to %s", addr);
	error_catch
	(
		tcp_attach(tcp);
		backup_process(self, client);
	);
	tcp_detach(tcp);

	event_signal(&self->on_complete);
	info("");
}

void
backup_run(Backup* self, Client* client)
{
	// detach client from current task
	tcp_detach(&client->tcp);
	self->client = client;

	// prepare on wait condition
	event_attach(&self->on_complete);

	// create backup worker
	task_create(&self->task, "backup", backup_main, self);

	// wait for backup completion
	cancel_pause();
	event_wait(&self->on_complete, -1);
	task_wait(&self->task);
	cancel_resume();
}

void
backup(Db* db, Client* client)
{
	// processs backup and wait for completion
	Backup backup;
	backup_init(&backup, db);
	error_catch(
		backup_run(&backup, client);
	);
	backup_free(&backup);
}
