
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

#if  0
void
backup_init(Backup* self, Storage* storage)
{
	self->state_step       = 0;
	self->state_step_total = 0;
	self->state_pos        = NULL;
	self->state_file       = NULL;
	self->snapshot         = false;
	self->client           = NULL;
	self->storage          = storage;
	event_init(&self->on_complete);
	wal_slot_init(&self->wal_slot);
	buf_init(&self->state);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	auto storage = self->storage;
	wal_del(&storage->wal_mgr.wal, &self->wal_slot);
	if (self->snapshot)
		id_mgr_delete(&storage->checkpoint_mgr.list_snapshot, 0);
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
	sfmt(path, sizeof(path), "%s/%s", state_directory(), name);
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
		sfmt(path, sizeof(path), "%s/%s", name, entry->d_name);
		backup_add(self, path);
	}
}

static int
backup_prepare_files(Backup* self, uint64_t checkpoint)
{
	auto storage = self->storage;
	auto buf     = &self->state;

	encode_array(buf);

	// include checkpoint files
	checkpoint_mgr_list(&storage->checkpoint_mgr, checkpoint, buf);

	// create wal snapshot and include wal files
	wal_snapshot(&storage->wal_mgr.wal, &self->wal_slot, buf);

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
	auto storage    = self->storage;
	auto checkpoint = checkpoint_mgr_snapshot(&storage->checkpoint_mgr);
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
	auto data = buf_create();
	defer_buf(data);
	encode_obj(data);
	// checkpoint
	encode_raw(data, "checkpoint", 10);
	encode_integer(data, checkpoint);
	// steps
	encode_raw(data, "steps", 5);
	encode_integer(data, self->state_step_total);
	encode_obj_end(data);

	// send reply
	auto client = self->client;
	auto reply  = &client->reply;
	auto pos    = data->start;
	json_export_pretty(&reply->content, runtime()->timezone, &pos);

	// reply application/json
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6,
	                            buf_size(&reply->content));
	http_end(buf);
	tcp_write_pair(&client->tcp, buf, &reply->content);
}

static void
backup_send_file(Backup* self, Str* name, int64_t size, Buf* data)
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
	buf_printf(buf, "Am-Step: %d\r\n", self->state_step);
	buf_printf(buf, "Am-File: %.*s\r\n", str_size(name), str_of(name));
	http_end(buf);
	tcp_write_buf(tcp, buf);

	// send preloaded data, if provided
	if (data)
	{
		tcp_write_buf(tcp, data);
		return;
	}

	// transfer file content
	buf = &reply->content;
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
	info(" %.*s (%.2f MiB)", str_size(&name), str_of(&name),
	     (double)size / 1024 / 1024);

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
		backup_next(self);
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
backup(Storage* storage, Client* client)
{
	// processs backup and wait for completion
	Backup backup;
	backup_init(&backup, storage);
	error_catch(
		backup_run(&backup, client);
	);
	backup_free(&backup);
}
#endif

void
backup(Storage* storage, Client* client)
{
	(void)storage;
	(void)client;
}
