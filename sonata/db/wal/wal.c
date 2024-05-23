
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
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>

void
wal_init(Wal* self)
{
	self->current = NULL;
	mutex_init(&self->lock);
	id_mgr_init(&self->list);
}

void
wal_free(Wal* self)
{
	if (self->current)
	{
		auto file = self->current;
		wal_file_close(file);
		wal_file_free(file);
		self->current = NULL;
	}
	id_mgr_free(&self->list);
	mutex_free(&self->lock);
}

static inline bool
wal_rotate_ready(Wal* self, uint64_t wm)
{
	return !self->current || self->current->file.size > wm;
}

void
wal_rotate(Wal* self, uint64_t wm)
{
	mutex_lock(&self->lock);
	if (! wal_rotate_ready(self, wm))
	{
		mutex_unlock(&self->lock);
		return;
	}

	uint64_t next_lsn = config_lsn() + 1;

	// create new wal file
	WalFile* file = NULL;
	Exception e;
	if (enter(&e))
	{
		file = wal_file_allocate(next_lsn);
		wal_file_create(file);
	}
	if (leave(&e))
	{
		if (file)
		{
			wal_file_close(file);
			wal_file_free(file);
		}
		mutex_unlock(&self->lock);
		rethrow();
	}

	// add to the list and set as current
	auto file_prev = self->current;
	self->current = file;

	id_mgr_add(&self->list, next_lsn);
	mutex_unlock(&self->lock);

	// sync and close prev file
	if (file_prev)
	{
		// todo: sync
		wal_file_close(file_prev);
		wal_file_free(file_prev);
	}
}

void
wal_gc(Wal* self, uint64_t snapshot)
{
	uint64_t min = snapshot;

	Buf list;
	buf_init(&list);
	guard(buf_free, &list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);

	// remove files by id
	if (list_count > 0)
	{
		char path[PATH_MAX];
		uint64_t *ids = (uint64_t*)list.start;
		int i = 0;
		for (; i < list_count; i++)
		{
			snprintf(path, sizeof(path), "%s/wal/%020" PRIu64 ".wal",
			         config_directory(),
			         ids[i]);
			log("wal: removing %s", path);
			fs_unlink("%s", path);
		}
	}
}

static inline int64_t
wal_file_id_of(const char* path)
{
	uint64_t id = 0;
	while (*path && *path != '.')
	{
		if (unlikely(! isdigit(*path)))
			return -1;
		id = (id * 10) + *path - '0';
		path++;
	}
	return id;
}

static void
wal_recover(Wal* self, char *path)
{
	// open and read log directory
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("wal: directory '%s' open error", path);
	guard(fs_opendir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		int64_t id = wal_file_id_of(entry->d_name);
		if (unlikely(id == -1))
			continue;
		id_mgr_add(&self->list, id);
	}
}

void
wal_open(Wal* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wal", config_directory());

	// create log directory
	if (! fs_exists("%s", path))
	{
		log("wal: new directory '%s'", path);
		fs_mkdir(0755, "%s", path);
	}

	// read file list
	wal_recover(self, path);

	// open last log file and set it as current
	if (self->list.list_count > 0)
	{
		uint64_t last = id_mgr_max(&self->list);
		self->current = wal_file_allocate(last);
		wal_file_open(self->current);
		file_seek_to_end(&self->current->file);
	} else
	{
		wal_rotate(self, 0);
	}
}

hot bool
wal_write(Wal* self, WalBatch* batch)
{
	mutex_lock(&self->lock);
	guard(mutex_unlock, &self->lock);

	// write wal file
	// todo: truncate on error

	// [header][rows meta][rows]
	wal_file_write(self->current, iov_pointer(&batch->iov), batch->iov.iov_count);
	list_foreach(&batch->list)
	{
		auto log_set = list_at(LogSet, link);
		wal_file_write(self->current, iov_pointer(&log_set->iov),
		               log_set->iov.iov_count);
	}

	// return true if wal is ready to be rotated
	auto wm = var_int_of(&config()->wal_rotate_wm);
	return wal_rotate_ready(self, wm);
}

void
wal_show(Wal* self, Buf* buf)
{
	int      list_count;
	uint64_t list_min;
	id_mgr_stats(&self->list, &list_count, &list_min);

	// map
	encode_map(buf, 7);

	// active
	encode_raw(buf, "active", 6);
	encode_bool(buf, var_int_of(&config()->wal));

	// rotate_wm
	encode_raw(buf, "rotate_wm", 9);
	encode_integer(buf, var_int_of(&config()->wal_rotate_wm));

	// sync_on_rotate
	encode_raw(buf, "sync_on_rotate", 14);
	encode_bool(buf, var_int_of(&config()->wal_sync_on_rotate));

	// sync_on_write
	encode_raw(buf, "sync_on_write", 13);
	encode_bool(buf, var_int_of(&config()->wal_sync_on_write));

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_integer(buf, config_lsn());

	// lsn_min
	encode_raw(buf, "lsn_min", 7);
	encode_integer(buf, list_min);

	// files
	encode_raw(buf, "files", 5);
	encode_integer(buf, list_count);
}
