
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
	self->slots_count = 0;
	mutex_init(&self->lock);
	id_mgr_init(&self->list);
	list_init(&self->slots);
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
	return !self->current || self->current->file.size >= wm;
}

static void
wal_swap(Wal* self)
{
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
		rethrow();
	}

	// add to the list and set as current
	auto file_prev = self->current;
	self->current = file;
	id_mgr_add(&self->list, next_lsn);

	// sync and close prev file
	if (file_prev)
	{
		// todo: sync
		wal_file_close(file_prev);
		wal_file_free(file_prev);
	}
}

void
wal_rotate(Wal* self)
{
	mutex_lock(&self->lock);
	guard(mutex_unlock, &self->lock);
	wal_swap(self);
}

static int
wal_slots(Wal* self, uint64_t* min)
{
	mutex_lock(&self->lock);
	int count = self->slots_count;
	list_foreach(&self->slots)
	{
		auto slot = list_at(WalSlot, link);
		uint64_t lsn = atomic_u64_of(&slot->lsn);
		if (*min < lsn)
			*min = lsn;
	}
	mutex_unlock(&self->lock);
	return count;
}

void
wal_gc(Wal* self, uint64_t min)
{
	wal_slots(self, &min);

	// remove wal files < min
	Buf list;
	buf_init(&list);
	guard(buf_free, &list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);
	if (list_count > 0)
	{
		auto id_list = buf_u64(&list);
		for (int i = 0; i < list_count; i++)
		{
			char path[PATH_MAX];
			snprintf(path, sizeof(path), "%s/wal/%020" PRIu64 ".wal",
			         config_directory(),
			         id_list[i]);
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
		wal_rotate(self);
	}
}

hot void
wal_write(Wal* self, WalBatch* batch)
{
	mutex_lock(&self->lock);
	guard(mutex_unlock, &self->lock);

	if (! var_int_of(&config()->wal))
	{
		config_lsn_next();
		return;
	}

	uint64_t next_lsn = config_lsn() + 1;
	batch->header.lsn = next_lsn;
	// todo: crc

	// create new wal file if needed
	if (wal_rotate_ready(self, var_int_of(&config()->wal_rotate_wm)))
		wal_swap(self);

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

	// update lsn globally
	config_lsn_set(next_lsn);

	// notify pending slots
	list_foreach(&self->slots)
	{
		auto slot = list_at(WalSlot, link);
		wal_slot_signal(slot, batch->header.lsn);
	}
}

void
wal_add(Wal* self, WalSlot* slot)
{
	assert(! slot->added);
	mutex_lock(&self->lock);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->added = true;
	mutex_unlock(&self->lock);
}

void
wal_del(Wal* self, WalSlot* slot)
{
	if (! slot->added)
		return;
	mutex_lock(&self->lock);
	list_unlink(&slot->link);
	self->slots_count--;
	slot->added = false;
	mutex_unlock(&self->lock);
}

void
wal_attach(Wal* self, WalSlot* slot)
{
	assert(! slot->on_write);
	auto on_write = condition_create();
	mutex_lock(&self->lock);
	slot->on_write = on_write;
	mutex_unlock(&self->lock);
}

void
wal_detach(Wal* self, WalSlot* slot)
{
	mutex_lock(&self->lock);
	auto on_write = slot->on_write;
	slot->on_write = NULL;
	mutex_unlock(&self->lock);
	if (on_write)
		condition_free(on_write);
}

bool
wal_in_range(Wal* self, uint64_t lsn)
{
	int      list_count;
	uint64_t list_min;
	id_mgr_stats(&self->list, &list_count, &list_min);
	return lsn >= list_min;
}

Buf*
wal_show(Wal* self)
{
	int      list_count;
	uint64_t list_min;
	id_mgr_stats(&self->list, &list_count, &list_min);

	uint64_t slots_min = UINT64_MAX;
	int      slots_count;
	slots_count = wal_slots(self, &slots_min);

	// map
	auto buf = buf_begin();
	encode_map(buf);

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

	// slots
	encode_raw(buf, "slots", 5);
	encode_integer(buf, slots_count);

	// slots_min
	encode_raw(buf, "slots_min", 9);
	encode_integer(buf, slots_min);

	encode_map_end(buf);
	return buf_end(buf);
}
