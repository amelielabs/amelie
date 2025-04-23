
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>

void
wal_init(Wal* self)
{
	self->dirfd       = -1;
	self->current     = NULL;
	self->slots_count = 0;
	mutex_init(&self->lock);
	id_mgr_init(&self->list);
	list_init(&self->slots);
}

void
wal_free(Wal* self)
{
	assert(! self->current);
	id_mgr_free(&self->list);
	mutex_free(&self->lock);
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

bool
wal_overflow(Wal* self)
{
	mutex_lock(&self->lock);
	defer(mutex_unlock, &self->lock);
	if (! self->current)
		return true;
	return self->current->file.size >= opt_int_of(&config()->wal_size);
}

void
wal_create(Wal* self, uint64_t lsn)
{
	// create new wal file
	WalFile* file = NULL;
	auto on_error = error_catch
	(
		file = wal_file_allocate(lsn);
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
	id_mgr_add(&self->list, lsn);

	// add to the list and set as current
	mutex_lock(&self->lock);
	auto file_prev = self->current;
	self->current = file;
	mutex_unlock(&self->lock);

	// close prev file
	if (! file_prev)
		return;
	error_catch (
		wal_file_close(file_prev);
	);
	wal_file_free(file_prev);
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
			         state_directory(),
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
wal_open_directory(Wal* self)
{
	// create directory
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wals", state_directory());
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// open and keep directory fd to support sync
	self->dirfd = vfs_open(path, O_DIRECTORY|O_RDONLY, 0);
	if (self->dirfd == -1)
		error_system();

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

static void
wal_truncate(Wal* self, uint64_t lsn)
{
	// if set, truncate logs by lsn
	if (lsn == 0)
		return;

	// find nearest file with id <= lsn
	auto id = id_mgr_find(&self->list, lsn);
	if (id == UINT64_MAX)
		id = id_mgr_min(&self->list);

	info("truncate wals (%" PRIu64 " lsn)", lsn);

	auto file = wal_file_allocate(id);
	defer(wal_file_close, file);
	wal_file_open(file);

	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// rewind to the lsn
	auto crc = opt_int_of(&config()->wal_crc);
	uint64_t offset = 0;
	for (;;)
	{
		buf_reset(&buf);
		if (! wal_file_pread(file, offset, &buf))
			break;

		// validate crc (header + commands)
		auto record = (Record*)(buf.start);
		if (crc)
		{
			if (unlikely(! record_validate(record)))
				error("wals/%" PRIu64 " (record crc mismatch)", id);
		}
		if (record->lsn > lsn)
			break;
		offset += record->size;
		if (record->lsn == lsn)
			break;
	}

	// truncate matched wal file to the record offset
	if (offset != file->file.size)
	{
		wal_file_truncate(file, offset);
		info("wals/%" PRIu64 " (truncated to %" PRIu64 " bytes)",
		     id, offset);
	}

	// remove all wal files after it
	for (;;)
	{
		id = id_mgr_next(&self->list, id);
		if (id == UINT64_MAX)
			break;
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s/wals/%" PRIu64,
		         state_directory(), id);
		fs_unlink("%s", path);
		info("wals/%" PRIu64 " (file removed)", id);
	}

	info("complete");
}

void
wal_open(Wal* self)
{
	// open or create directory and add existing files
reopen:
	wal_open_directory(self);

	// create new wal file
	if (! self->list.list_count)
	{
		wal_create(self, 1);
		if (opt_int_of(&config()->wal_sync_on_create))
			wal_sync(self, true);
		return;
	}

	// truncate wals to the specified lsn, if set
	uint64_t lsn = opt_int_of(&config()->wal_truncate);
	if (lsn != 0)
	{
		wal_truncate(self, lsn);
		wal_close(self);
		opt_int_set(&config()->wal_truncate, 0);
		goto reopen;
	}

	// open last log file and set it as current
	uint64_t last = id_mgr_max(&self->list);
	self->current = wal_file_allocate(last);
	wal_file_open(self->current);
	file_seek_to_end(&self->current->file);
}

void
wal_close(Wal* self)
{
	if (self->dirfd != -1)
	{
		close(self->dirfd);
		self->dirfd = -1;
	}
	if (self->current)
	{
		auto file = self->current;
		error_catch( wal_file_close(file) );
		wal_file_free(file);
		self->current = NULL;
	}
	id_mgr_reset(&self->list);
}

hot static inline uint64_t
wal_write_list(Wal* self, WriteList* list)
{
	auto current = self->current;
	auto lsn = state_lsn();
	list_foreach(&list->list)
	{
		auto write = list_at(Write, link);

		// update stats
		opt_int_add(&state()->writes, 1);
		opt_int_add(&state()->writes_bytes, write->header.size);
		opt_int_add(&state()->ops, write->header.ops);

		// finilize wal record
		lsn++;
		write_end(write, lsn);

		// write wal file

		// [header][commands][rows or ops]
		wal_file_write(current, &write->iov);
		list_foreach(&write->list)
		{
			auto write_log = list_at(WriteLog, link);
			wal_file_write(current, &write_log->iov);
		}
	}

	return lsn;
}

hot bool
wal_write(Wal* self, WriteList* list)
{
	mutex_lock(&self->lock);
	defer(mutex_unlock, &self->lock);

	// do atomical write of a list of wal records
	uint64_t lsn;
	auto current = self->current;
	auto current_offset = current->file.size;
	auto on_error = error_catch(
		lsn = wal_write_list(self, list)
	);
	if (unlikely(on_error)) {
		if (error_catch(wal_file_truncate(current, current_offset)))
			panic("wal: failed to truncate wal file after failed write");
		rethrow();
	}

	// update lsn globally
	state_lsn_set(lsn);

	// notify pending slots
	list_foreach(&self->slots)
	{
		auto slot = list_at(WalSlot, link);
		wal_slot_signal(slot, lsn);
	}

	return self->current->file.size >= opt_int_of(&config()->wal_size);
}

hot void
wal_sync(Wal* self, bool sync_dir)
{
	wal_file_sync(self->current);
	if (! sync_dir)
		return;
	auto rc = vfs_fsync(self->dirfd);
	if (rc == -1)
		error_system();
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
			size = fs_size("%s/wals/%" PRIu64, state_directory(), id);
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
	encode_integer(buf, state_lsn());

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
	encode_integer(buf, opt_int_of(&state()->writes));

	// writes_bytes
	encode_raw(buf, "writes_bytes", 12);
	encode_integer(buf, opt_int_of(&state()->writes_bytes));

	// ops
	encode_raw(buf, "ops", 3);
	encode_integer(buf, opt_int_of(&state()->ops));

	// checkpoint
	encode_raw(buf, "checkpoint", 10);
	encode_integer(buf, state_checkpoint());

	encode_obj_end(buf);
	return buf;
}
