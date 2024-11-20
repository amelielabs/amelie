
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>

void
backup_init(Backup* self, Db* db)
{
	self->snapshot = false;
	self->client   = NULL;
	self->db       = db;
	event_init(&self->on_complete);
	wal_slot_init(&self->wal_slot);
	buf_init(&self->state);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	auto db = self->db;
	wal_del(&db->wal, &self->wal_slot);
	if (self->snapshot)
		id_mgr_delete(&db->checkpoint_mgr.list_snapshot, 0);
	event_detach(&self->on_complete);
	buf_free(&self->state);
	task_free(&self->task);
}

static inline void
backup_list(Buf* self, char* name)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s", config_directory(), name);
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	guard(fs_opendir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		snprintf(path, sizeof(path), "%s/%s", name, entry->d_name);
		encode_array(self);
		// path
		encode_cstr(self, path);
		// size
		auto size = fs_size("%s/%s/%s", config_directory(), name, entry->d_name);
		if (size == -1)
			error_system();
		encode_integer(self, size);
		encode_array_end(self);
	}
}

static void
backup_prepare(Backup* self)
{
	// read config file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
	Buf config_data;
	buf_init(&config_data);
	guard_buf(&config_data);
	file_import(&config_data, "%s", path);
	Str config_str;
	buf_str(&config_data, &config_str);

	// prepare backup state
	auto buf = &self->state;
	encode_obj(buf);

	// add checkpoint snapshot
	auto db = self->db;
	uint64_t checkpoint = checkpoint_mgr_snapshot(&db->checkpoint_mgr);
	self->snapshot = true;

	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, checkpoint);

	// files
	encode_raw(buf, "files", 5);
	encode_array(buf);

	// checkpoint files
	checkpoint_mgr_list(&db->checkpoint_mgr, checkpoint, buf);

	// create wal snapshot and include wal files
	wal_snapshot(&db->wal, &self->wal_slot, buf);

	// certs
	backup_list(buf, "certs");
	encode_array_end(buf);

	// config
	encode_raw(buf, "config", 6);
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, &config_str, buf);

	encode_obj_end(buf);
}

static void
backup_join(Backup* self)
{
	// create backup state
	control_lock();

	Exception e;
	if (enter(&e))
		backup_prepare(self);

	control_unlock();
	if (leave(&e))
	{ }

	auto client = self->client;
	auto reply = &client->reply;
	body_add_raw(&reply->content, &self->state, global()->timezone);
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%d", buf_size(&reply->content));
	http_write(reply, "Content-Type", "application/json");
	http_write_end(reply);
	tcp_write_pair(&client->tcp, &reply->raw, &reply->content);
}

static void
backup_send(Backup* self)
{
	auto client = self->client;
	auto request = &client->request;

	// Am-File
	auto hdr_path = http_find(request, "Am-File", 7);
	if (unlikely(! hdr_path))
		error("Am-File field is missing");

	// Am-File-Size
	auto hdr_size = http_find(request, "Am-File-Size", 12);
	if (unlikely(! hdr_size))
		error("Am-File-Size field is missing");
	int64_t size;
	if (str_toint(&hdr_size->value, &size) == -1)
		error("Am-File-Size field is invalid");

	// todo: validate path
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", config_directory(),
	         str_size(&hdr_path->value),
	         str_of(&hdr_path->value));
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open(&file, path);

	// requested size must be <= to the current file size
	if ((int64_t)file.size < size)
		error("Am-File-Size field is invalid (file size mismatch)");

	info("%.*s (%" PRIu64 " bytes)", str_size(&hdr_path->value),
	     str_of(&hdr_path->value), size);

	// prepare and send header
	auto reply = &client->reply;
	auto tcp   = &client->tcp;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%" PRIu64, size);
	http_write(reply, "Content-Type", "application/octet-stream");
	http_write_end(reply);
	tcp_write_buf(tcp, &reply->raw);

	// transfer file content
	auto buf = &reply->content;
	while (size > 0)
	{
		buf_reset(buf);
		int64_t chunk = 256 * 1024;
		if (size < chunk)
			chunk = size;
		file_read_buf(&file, buf, chunk);
		tcp_write_buf(tcp, buf);
		buf_reset(buf);
		size -= chunk;
	}
}

static void
backup_main(void* arg)
{
	Backup* self   = arg;
	auto    client = self->client;
	auto    tcp    = &client->tcp;

	info("begin");

	Exception e;
	if (enter(&e))
	{
		tcp_attach(tcp);

		// create and send backup state
		backup_join(self);

		// process file copy requests (till disconnect)
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

	tcp_detach(tcp);
	if (leave(&e))
	{ }

	event_signal(&self->on_complete);
	info("complete");
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
	info("begin backup");

	// processs backup and wait for completion
	Backup backup;
	backup_init(&backup, db);
	Exception e;
	if (enter(&e)) {
		backup_run(&backup, client);
	}
	if (leave(&e))
	{ }
	backup_free(&backup);
}
