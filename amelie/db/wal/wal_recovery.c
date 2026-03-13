
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>

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

static inline void
wal_file_add(Wal* self, WalFile* file)
{
	// order by id
	auto     list = self->files;
	WalFile* prev = NULL;
	while (list)
	{
		if (file->id < list->id) 
			break;
		prev = list;
		list = list->next;
	}
	if (prev)
		prev->next = file;
	else
		self->files = file;
	file->next = list;
	self->files_count++;

	wal_file_pin(file);
}

static void
wal_open_directory(Wal* self)
{
	// create directory
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/wal", state_directory());
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// open and keep directory fd to support sync
	self->dirfd = vfs_open(path, O_DIRECTORY|O_RDONLY, 0);
	if (self->dirfd == -1)
		error_system();

	// open and read log directory
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("wal: directory '%s' open error", path);
	defer(fs_closedir_defer, dir);
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
		auto file = wal_file_allocate(id);
		wal_file_add(self, file);
		wal_file_open(file);
		file_seek_to_end(&file->file);
	}

	// set current (max)
	if (self->files_count > 0)
	{
		self->current = self->files;
		while (self->current->next)
			self->current = self->current->next;
	}
}

static void
wal_truncate(Wal* self, uint64_t lsn)
{
	// if set, truncate logs by lsn
	if (lsn == 0)
		return;
	(void)self;

#if 0
	// find nearest file with id <= lsn
	auto id = id_mgr_find(&self->list, lsn);
	if (id == UINT64_MAX)
		id = id_mgr_min(&self->list);

	info("wal: truncate wal (%" PRIu64 " lsn)", lsn);

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
				error("wal/%" PRIu64 " (record crc mismatch)", id);
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
		info(" %" PRIu64 " (truncated to %" PRIu64 " bytes)",
		     id, offset);
	}

	// remove all wal files after it
	for (;;)
	{
		id = id_mgr_next(&self->list, id);
		if (id == UINT64_MAX)
			break;
		char path[PATH_MAX];
		sfmt(path, sizeof(path), "%s/wal/%" PRIu64,
		     state_directory(), id);
		fs_unlink("%s", path);
		info(" %" PRIu64 " (file removed)", id);
	}

	info("");
#endif
}

void
wal_recovery(Wal* self)
{
	// open or create directory and add existing files
	wal_open_directory(self);

	// create new wal file
	if (! self->files_count)
	{
		// create new wal file
		self->files = wal_file_allocate(1);
		wal_file_create(self->files);
		self->files_count++;
		self->current = self->files;
		wal_file_pin(self->current);

		state_lsn_set(0);
		return;
	}

	// truncate wals to the specified lsn, if set
	uint64_t lsn = opt_int_of(&config()->wal_truncate);
	if (! lsn)
		return;
	wal_truncate(self, lsn);
	opt_int_set(&config()->wal_truncate, 0);
}
