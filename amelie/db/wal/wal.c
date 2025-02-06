
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>

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
wal_rotate_ready(Wal* self, uint64_t size)
{
	return !self->current || self->current->file.size >= size;
}

static void
wal_swap(Wal* self)
{
	uint64_t next_lsn = config_lsn() + 1;

	// create new wal file
	WalFile* file = NULL;
	auto on_error = error_catch
	(
		file = wal_file_allocate(next_lsn);
		wal_file_create(file);
	);
	if (on_error)
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
	defer_buf(&list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);
	if (list_count > 0)
	{
		size_t size  = 0;
		auto id_list = buf_u64(&list);
		for (int i = 0; i < list_count; i++)
		{
			char path[PATH_MAX];
			snprintf(path, sizeof(path), "%s/wals/%" PRIu64,
			         config_directory(),
			         id_list[i]);
			size += fs_size("%s", path);
			fs_unlink("%s", path);
		}
		info("wal: %d files removed (%.2f MiB)", list_count,
		     (double)size / 1024 / 1024);
	}
}

static inline int64_t
wal_file_id_of(const char* path)
{
	int64_t id = 0;
	while (*path)
	{
		if (unlikely(! isdigit(*path)))
			return -1;
		if (unlikely(int64_mul_add_overflow(&id, id, 10, *path - '0')))
			return -1;
		path++;
	}
	return id;
}

static void
wal_recover(Wal* self, char* path)
{
	// open and read log directory
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("wal: directory '%s' open error", path);
	defer(fs_opendir_defer, dir);
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
	// create directory
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wals", config_directory());
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

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
		wal_swap(self);
	}
}

hot void
wal_write(Wal* self, WalBatch* batch)
{
	mutex_lock(&self->lock);
	defer(mutex_unlock, &self->lock);

	// update stats
	var_int_add(&state()->writes, 1);
	var_int_add(&state()->writes_bytes, batch->header.size);
	var_int_add(&state()->ops, batch->header.count);

	uint64_t next_lsn = config_lsn() + 1;
	batch->header.lsn = next_lsn;
	// todo: crc

	// create new wal file if needed
	if (wal_rotate_ready(self, var_int_of(&config()->wal_size)))
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
	mutex_lock(&self->lock);
	event_attach(&slot->on_write);
	mutex_unlock(&self->lock);
}

void
wal_detach(Wal* self, WalSlot* slot)
{
	mutex_lock(&self->lock);
	event_detach(&slot->on_write);
	mutex_unlock(&self->lock);
}

void
wal_snapshot(Wal* self, WalSlot* slot, Buf* buf)
{
	mutex_lock(&self->lock);
	defer(mutex_unlock, &self->lock);

	spinlock_lock(&self->list.lock);
	defer(spinlock_unlock, &self->list.lock);

	// create wal slot to ensure listed files exists
	wal_slot_set(slot, 0);
	list_append(&self->slots, &slot->link);
	self->slots_count++;
	slot->added = true;

	for (int i = 0; i < self->list.list_count; i++)
	{
		auto id = buf_u64(&self->list.list)[i];
		encode_array(buf);

		// path
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "wals/%" PRIu64, id);
		encode_cstr(buf, path);

		// size
		int64_t size;
		if (id == self->current->id) {
			size = self->current->file.size;
		} else
		{
			size = fs_size("%s/wals/%" PRIu64, config_directory(), id);
			if (size == -1)
				error_system();
		}
		encode_integer(buf, size);
		encode_array_end(buf);
	}
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
wal_status(Wal* self)
{
	int      list_count;
	uint64_t list_min;
	id_mgr_stats(&self->list, &list_count, &list_min);

	uint64_t slots_min = UINT64_MAX;
	int      slots_count;
	slots_count = wal_slots(self, &slots_min);

	// obj
	auto buf = buf_create();
	encode_obj(buf);

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

	// writes
	encode_raw(buf, "writes", 6);
	encode_integer(buf, var_int_of(&state()->writes));

	// writes_bytes
	encode_raw(buf, "writes_bytes", 12);
	encode_integer(buf, var_int_of(&state()->writes_bytes));

	// ops
	encode_raw(buf, "ops", 3);
	encode_integer(buf, var_int_of(&state()->ops));

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, config_checkpoint());

	encode_obj_end(buf);
	return buf;
}
