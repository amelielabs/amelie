
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>
#include <amelie_db.h>

typedef struct Backup Backup;

struct Backup
{
	Uuid*          id;
	Buf            config;
	Buf            state;
	CheckpointRef* checkpoint;
	Buf            wal_files;
	WalSlot        wal_snapshot;
	Db*            db;
};

static void
backup_init(Backup* self, Db* db, Uuid* id)
{
	self->id         = id;
	self->checkpoint = NULL;
	self->db         = db;
	buf_init(&self->config);
	buf_init(&self->state);
	buf_init(&self->wal_files);
	wal_slot_init(&self->wal_snapshot);
}

static void
backup_free(Backup* self)
{
	auto db = self->db;

	// detach wal slot
	wal_detach(&db->wal, &self->wal_snapshot);

	// detach checkpoint
	if (self->checkpoint)
		checkpoints_unref(&db->checkpoints, self->checkpoint);

	buf_free(&self->config);
	buf_free(&self->state);
	buf_free(&self->wal_files);
}

static void
backup_prepare(Backup* self)
{
	auto lock_catalog = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	defer(unlock, lock_catalog);

	// config
	auto buf = opts_list_persistent(&runtime()->config.opts);
	defer_buf(buf);
	buf_write_buf(&self->config, buf);

	// state
	control_state_read(&self->state);

	// take checkpoint snapshot
	self->checkpoint = checkpoints_ref(&self->db->checkpoints);

	// get wal snapshot and the file list
	wal_snapshot(&self->db->wal, &self->wal_snapshot, &self->wal_files);
}

static void
backup_file(char* path_base, char* path_relative, Buf* data)
{
	// <base>/backup/id.incomplete/<path_relative>
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/{s}", path_base, path_relative);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	if (! buf_empty(data))
		file_write_buf(&file, data);

	info("backup: {s}", path_relative);
}

static void
backup_file_json(char* path_base, char* path_relative, Buf* data)
{
	// <base>/backup/id.incomplete/<path_relative>
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/{s}", path_base, path_relative);

	// convert to json
	auto buf = buf_create();
	defer_buf(buf);
	auto pos = data->start;
	json_export_pretty(buf, NULL, &pos);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	file_write_buf(&file, buf);

	info("backup: {s}", path_relative);
}

static void
backup_main(Backup* self)
{
	char id[UUID_SZ];
	uuid_get(self->id, id, sizeof(id));

	// backup already exists 
	if (fs_exists("{s}/backup/{s}", state_directory(), id))
	{
		info("backup: already exists");
		return;
	}

	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/backup/{s}.incomplete",
	       state_directory(), id);

	info("backup: {s}", id);

	// create directories
	fs_mkdir(0755, "{s}", path);
	fs_mkdir(0755, "{s}/security", path);
	fs_mkdir(0755, "{s}/backup", path);
	fs_mkdir(0755, "{s}/checkpoint", path);
	fs_mkdir(0755, "{s}/checkpoint/{u64}", path, state_checkpoint());
	fs_mkdir(0755, "{s}/wal", path);

	// amelie.config
	backup_file_json(path, "amelie.config", &self->config);

	// amelie.state
	backup_file(path, "amelie.state", &self->state);

	// create checkpoint files (hardlinks)
	checkpoints_backup(self->checkpoint, path);

	// create wal files (hardlinks and copy)
	wal_backup(&self->wal_files, path);

	// rename as complete
	fs_rename(path,  "{s}/backup/{s}", state_directory(), id);

	info("backup: done");
}

static void
backup_job(intptr_t* argv)
{
	auto self = (Backup*)argv[0];
	backup_main(self);
}

void
backup(Db* self, Uuid* id)
{
	Backup backup;
	backup_init(&backup, self, id);
	defer(backup_free, &backup);
	backup_prepare(&backup);
	run(backup_job, 1, &backup);
}
