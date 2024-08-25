
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>

void
backup_init(Backup* self, Db* db)
{
	self->checkpoint_snapshot = -1;
	self->on_complete         = NULL;
	self->client              = NULL;
	self->db                  = db;
	wal_slot_init(&self->wal_slot);
	buf_init(&self->state);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	auto db = self->db;
	wal_del(&db->wal, &self->wal_slot);
	if (self->checkpoint_snapshot != -1)
		id_mgr_delete(&db->checkpoint_mgr.list_snapshot, 0);
	if (self->on_complete)
		condition_free(self->on_complete);
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
		encode_cstr(self, path);
	}
}

static void
backup_prepare_state(Backup* self)
{
	// read config file
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/config.json", config_directory());
	Buf config_data;
	buf_init(&config_data);
	guard(buf_free, &config_data);
	file_import(&config_data, "%s", path);
	Str config_str;
	buf_str(&config_data, &config_str);

	// prepare backup state
	auto buf = &self->state;
	encode_map(buf);

	// checkpoint
	uint64_t checkpoint = config_checkpoint();
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, checkpoint);

	// files
	encode_raw(buf, "files", 5);
	encode_array(buf);

	// list files for backup (checkpoint and wal)
	if (checkpoint > 0)
	{
		snprintf(path, sizeof(path), "%" PRIu64, checkpoint);
		backup_list(buf, path);
	}
	backup_list(buf, "wal");
	encode_array_end(buf);

	// config
	encode_raw(buf, "config", 6);
	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, global()->timezone, &config_str, buf);

	encode_map_end(buf);
}

static void
backup_prepare(Backup* self)
{
	auto db = self->db;

	// take exclusive lock
	control_lock();

	Exception e;
	if (enter(&e))
	{
		// create backup state
		backup_prepare_state(self);

		// create new wal
		wal_rotate(&db->wal);

		// create wal slot
		wal_slot_set(&self->wal_slot, 0);
		wal_add(&db->wal, &self->wal_slot);

		// add checkpoint snapshot
		id_mgr_add(&db->checkpoint_mgr.list_snapshot, 0);
		self->checkpoint_snapshot = 0;
	}

	control_unlock();
	if (leave(&e))
	{ }
}

static void
backup_send(Backup* self, Str* url)
{
	// open file (partition or wal file)
	if (! str_compare_raw_prefix(url, "/backup/", 8))
		error("backup: unexpected request");
	str_advance(url, 8);

	// todo: validate path
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%.*s", config_directory(),
	         str_size(url), str_of(url));
	File file;
	file_init(&file);
	guard(file_close, &file);
	file_open(&file, path);

	info("file %.*s (%" PRIu64 " bytes)", str_size(url),
	     str_of(url), file.size);

	// prepare and send header
	auto reply = &self->client->reply;
	auto tcp   = &self->client->tcp;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%" PRIu64, file.size);
	http_write(reply, "Content-Type", "application/octet-stream");
	http_write_end(reply);
	tcp_write_buf(tcp, &reply->raw);

	// transfer file content
	auto buf = &reply->content;
	for (uint64_t size = file.size; size > 0;)
	{
		buf_reset(buf);
		uint64_t chunk = 256 * 1024;
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
		// create backup state
		backup_prepare(self);
		tcp_attach(tcp);

		// send backup state
		auto reply = &client->reply;
		body_add_raw(&reply->content, &self->state, global()->timezone);
		http_write_reply(reply, 200, "OK");
		http_write(reply, "Content-Length", "%d", buf_size(&reply->content));
		http_write(reply, "Content-Type", "application/json");
		http_write_end(reply);
		tcp_write_pair(tcp, &reply->raw, &reply->content);

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
			auto url = &request->options[HTTP_URL];
			backup_send(self, url);
		}
	}

	tcp_detach(tcp);
	if (leave(&e))
	{ }

	condition_signal(self->on_complete);
	info("complete");
}

void
backup_run(Backup* self, Client* client)
{
	// detach client from current task
	tcp_detach(&client->tcp);
	self->client = client;

	// prepare on wait condition
	self->on_complete = condition_create();

	// create backup worker
	task_create(&self->task, "backup", backup_main, self);

	// wait for backup completion
	coroutine_cancel_pause(am_self());
	condition_wait(self->on_complete, -1);
	coroutine_cancel_resume(am_self());

	task_wait(&self->task);
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
