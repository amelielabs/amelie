
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

void
backup_init(Backup* self, Db* db)
{
	self->state_step       = 0;
	self->state_step_total = 0;
	self->state_pos        = NULL;
	self->state_file       = NULL;
	self->snapshot         = false;
	self->client           = NULL;
	self->db               = db;
	event_init(&self->on_complete);
	wal_slot_init(&self->wal_slot);
	buf_init(&self->state);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	auto db = self->db;
	wal_del(&db->wal_mgr.wal, &self->wal_slot);
	if (self->snapshot)
		id_mgr_delete(&db->checkpoint_mgr.list_snapshot, 0);
	event_detach(&self->on_complete);
	if (self->state_file)
		buf_free(self->state_file);
	buf_free(&self->state);
	task_free(&self->task);
}

static inline void
backup_add(Buf* self, char* path)
{
	encode_array(self);
	// relative path
	encode_cstr(self, path);
	// size
	auto size = fs_size("%s/%s", state_directory(), path);
	if (size == -1)
		error_system();
	encode_integer(self, size);
	encode_array_end(self);
}

static inline void
backup_list(Buf* self, char* name)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s", state_directory(), name);
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_opendir_defer, dir);
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
		backup_add(self, path);
	}
}

static int
backup_prepare_files(Backup* self, uint64_t checkpoint)
{
	auto db  = self->db;
	auto buf = &self->state;

	encode_array(buf);

	// include checkpoint files
	checkpoint_mgr_list(&db->checkpoint_mgr, checkpoint, buf);

	// create wal snapshot and include wal files
	wal_snapshot(&db->wal_mgr.wal, &self->wal_slot, buf);

	// include certs files
	backup_list(buf, "certs");

	// include files
	backup_add(buf, "version.json");
	backup_add(buf, "config.json");
	backup_add(buf, "state.json");

	// read state file into memory
	self->state_file = file_import("%s/state.json", state_directory());

	encode_array_end(buf);
	return json_array_size(self->state.start);
}

static uint64_t
backup_prepare(Backup* self)
{
	// add checkpoint snapshot
	auto db  = self->db;
	auto checkpoint = checkpoint_mgr_snapshot(&db->checkpoint_mgr);
	self->snapshot = true;

	// create wal snapshot and prepare files
	self->state_step_total = backup_prepare_files(self, checkpoint);
	self->state_step       = 0;
	self->state_pos        = self->state.start;
	json_read_array(&self->state_pos);
	return checkpoint;
}

static void
backup_join(Backup* self)
{
	// create backup state
	uint64_t checkpoint;
	control_lock();
	auto on_error = error_catch(
		checkpoint = backup_prepare(self)
	);
	control_unlock();
	if (on_error)
		rethrow();

	// prepare backup state reply
	auto buf = buf_create();
	defer_buf(buf);
	encode_obj(buf);
	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, checkpoint);
	// steps
	encode_raw(buf, "steps", 5);
	encode_integer(buf, self->state_step_total);
	encode_obj_end(buf);

	// send reply
	auto client = self->client;
	auto reply = &client->reply;
	uint8_t* pos = buf->start;
	json_export_pretty(&reply->content, global()->timezone, &pos);
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%d", buf_size(&reply->content));
	http_write(reply, "Content-Type", "application/json");
	http_write(reply, "Am-Service", "backup");
	http_write(reply, "Am-Version", "1");
	http_write_end(reply);
	tcp_write_pair(&client->tcp, &reply->raw, &reply->content);
}

static inline void
backup_validate_headers(Client* client)
{
	auto request = &client->request;

	// Am-Service
	auto am_service = http_find(request, "Am-Service", 10);
	if (unlikely(! am_service))
		error("backup Am-Service field is missing");
	if (unlikely(! str_is(&am_service->value, "backup", 6)))
		error("backup Am-Service is invalid");

	// Am-Version
	auto am_version = http_find(request, "Am-Version", 10);
	if (unlikely(! am_version))
		error("backup Am-Version field is missing");
	if (unlikely(! str_is(&am_version->value, "1", 1)))
		error("backup Am-Version is invalid");
}

static void
backup_send_file(Backup* self, Str* name, int64_t size, Buf* data)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", state_directory(),
	         str_size(name), str_of(name));

	// prepare and send header
	auto client = self->client;
	auto reply  = &client->reply;
	auto tcp    = &client->tcp;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%" PRIu64, size);
	http_write(reply, "Content-Type", "application/octet-stream");
	http_write(reply, "Am-Service", "backup");
	http_write(reply, "Am-Version", "1");
	http_write(reply, "Am-Step", "%d", self->state_step);
	http_write(reply, "Am-File", "%.*s", str_size(name), str_of(name));
	http_write_end(reply);
	tcp_write_buf(tcp, &reply->raw);

	// send preloaded data, if provided
	if (data)
	{
		tcp_write_buf(tcp, data);
		return;
	}

	// transfer file content
	auto buf = &reply->content;
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open(&file, path);
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
backup_next(Backup* self)
{
	auto client = self->client;
	auto request = &client->request;

	// Am-Step
	auto am_step = http_find(request, "Am-Step", 7);
	if (unlikely(! am_step))
		error("backup Am-Step field is missing");
	int64_t step;
	if (str_toint(&am_step->value, &step) == -1)
		error("backup Am-Step is invalid");

	// expect correct next step order (starting from 0)
	if (step != self->state_step || step >= self->state_step_total )
		error("backup Am-Step order is invalid");

	// [path, size]
	uint8_t** pos = &self->state_pos;
	json_read_array(pos);
	Str name;
	json_read_string(pos, &name);
	int64_t size;
	json_read_integer(pos, &size);
	json_read_array_end(pos);
	info("%.*s (%" PRIu64 " bytes)", str_size(&name),
	     str_of(&name), size);

	// on the last state, send preloaded state file content
	Buf* buf = NULL;
	if (step == self->state_step_total - 1)
		buf = self->state_file;

	// transmit file
	backup_send_file(self, &name, size, buf);

	// advance step
	self->state_step++;
}

static inline void
backup_process(Backup* self, Client* client)
{
	// validate headers
	backup_validate_headers(client);

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
		backup_validate_headers(client);
		backup_next(self);
	}
}

static void
backup_main(void* arg)
{
	Backup* self   = arg;
	auto    client = self->client;
	auto    tcp    = &client->tcp;

	info("");
	info("⟶ backup");
	error_catch
	(
		tcp_attach(tcp);
		backup_process(self, client);
	);
	tcp_detach(tcp);

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
	// processs backup and wait for completion
	Backup backup;
	backup_init(&backup, db);
	error_catch(
		backup_run(&backup, client);
	);
	backup_free(&backup);
}
