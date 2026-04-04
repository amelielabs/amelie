
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
	self->db       = db;
	self->client   = NULL;
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
backup_push(Backup* self, Str* name, size_t size)
{
	info("backup: %.*s (%.2f MiB)", str_size(name), str_of(name),
	     (double)size / 1024 / 1024);

	// todo: validate name

	// open file
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%.*s", state_directory(),
	     str_size(name), str_of(name));

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);
	if (size > file.size)
		error("backup: requested file '%.*s'' size mismatch",
		      str_size(name), str_of(name));

	// [file, size]
	auto buf = &self->client->reply.content;
	buf_reset(buf);
	encode_array(buf);
	encode_string(buf, name);
	encode_integer(buf, size);
	encode_array_end(buf);

	// transfer file content
	WsFrame frame;
	backup_send(self->client, &frame, true, BACKUP_PUSH, buf, size);

	auto chunk = 256u * 1024;
	while (size > 0)
	{
		buf_reset(buf);
		if (size < chunk)
			chunk = size;
		file_read_buf(&file, buf, chunk);
		tcp_write_buf(&self->client->tcp, buf);
		size -= chunk;
	}
}

static inline void
backup_process(Backup* self)
{
	auto client = self->client;

	// create db snapshot
	self->snapshot = db_snapshot(self->db);

	// create and send snapshot data
	WsFrame frame;
	backup_send(client, &frame, true, BACKUP_JOIN,
	            &self->snapshot->data, 0);

	auto buf = &self->client->request.content;
	for (;;)
	{
		buf_reset(buf);

		WsFrame frame;
		auto op = backup_recv(client, &frame, buf);
		if (op == BACKUP_DONE)
			break;

		// file request
		if (op == BACKUP_PULL)
		{
			// [file, size]
			auto pos = buf->start;
			Str     name;
			int64_t size;
			json_read_array(&pos);
			json_read_string(&pos, &name);
			json_read_integer(&pos, &size);
			json_read_array_end(&pos);
			backup_push(self, &name, size);
			continue;
		}

		error("backup: unexpected response");
		break;
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

		// do websocket handshake
		backup_accept(client);
		backup_process(self);
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
