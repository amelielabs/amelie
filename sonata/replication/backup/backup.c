
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
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>

static void
backup_send(Backup* self, Str* url)
{
	auto buf = &self->buf;	
	buf_reset(buf);

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

	log("%.*s (%" PRIu64 " bytes)", str_size(url), str_of(url), file.size);

	// prepare header
	reply_create_header(buf, 200, "OK", "application/octet-stream",
	                    file.size);

	// transfer file content
	uint64_t size = file.size;
	if (size == 0)
	{
		tcp_write_buf(self->tcp, buf);
		return;
	}
	while (size > 0)
	{
		uint64_t chunk = 256 * 1024;
		if (size < chunk)
			chunk = size;
		file_read_buf(&file, buf, chunk);
		tcp_write_buf(self->tcp, buf);
		buf_reset(buf);
		size -= chunk;
	}
}

static void
backup_main(void* arg)
{
	Backup* self = arg;
	auto tcp = self->tcp;

	log("begin");

	Exception e;
	if (enter(&e))
	{
		tcp_attach(tcp);

		// create backup state
		rpc(global()->control->system, RPC_BACKUP, 1, self);

		// send backup state
		Body body;
		body_init(&body);
		guard(body_free, &body);
		body_add_buf(&body, &self->buf_state);

		Reply reply;
		reply_init(&reply);
		guard(reply_free, &reply);
		reply_create(&reply, 200, "OK", &body.buf);
		reply_write(&reply, tcp);

		// process file copy requests (till disconnect)
		Http request;
		http_init(&request, HTTP_REQUEST, 1024);
		guard(http_free, &request);
		for (;;)
		{
			http_reset(&request);
			auto eof = http_read(&request, tcp);
			if (eof)
				break;
			http_read_content(&request, tcp, &request.content);
			backup_send(self, &request.url);
		}
	}

	if (leave(&e))
	{ }

	condition_signal(self->on_complete);
	log("complete");
}

void
backup_init(Backup* self, Db* db)
{
	self->checkpoint_snapshot = -1;
	self->on_complete         = NULL;
	self->tcp                 = NULL;
	self->db                  = db;
	wal_slot_init(&self->wal_slot);
	buf_init(&self->buf_state);
	buf_init(&self->buf);
	task_init(&self->task);
}

void
backup_free(Backup* self)
{
	auto db = self->db;
	wal_detach(&db->wal, &self->wal_slot);
	if (self->checkpoint_snapshot != -1)
		id_mgr_delete(&db->checkpoint_mgr.list_snapshot, 0);
	if (self->on_complete)
		condition_free(self->on_complete);
	buf_free(&self->buf_state);
	buf_free(&self->buf);
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
	auto buf = &self->buf_state;
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
	json_parse(&json, &config_str, buf);

	encode_map_end(buf);
}

void
backup_prepare(Backup* self)
{
	auto db = self->db;

	// create backup state
	backup_prepare_state(self);

	// create new wal
	wal_rotate(&db->wal, 0);

	// create wal slot
	wal_slot_set(&self->wal_slot, 0);
	wal_attach(&db->wal, &self->wal_slot);

	// add checkpoint snapshot
	id_mgr_add(&db->checkpoint_mgr.list_snapshot, 0);
	self->checkpoint_snapshot = 0;
}

void
backup_run(Backup* self, Tcp* tcp)
{
	// detach client from current task
	tcp_detach(tcp);
	self->tcp = tcp;

	// prepare on wait condition
	self->on_complete = condition_create();

	// create backup worker
	task_create(&self->task, "backup", backup_main, self);

	// wait for backup completion
	coroutine_cancel_pause(so_self());
	condition_wait(self->on_complete, -1);
	coroutine_cancel_resume(so_self());

	task_wait(&self->task);
}

void
backup(Db* db, Tcp* tcp)
{
	log("begin backup");

	// processs backup and wait for completion
	Backup backup;
	backup_init(&backup, db);

	Exception e;
	if (enter(&e)) {
		backup_run(&backup, tcp);
	}

	if (leave(&e))
	{ }

	backup_free(&backup);
	tcp_close(tcp);
}
