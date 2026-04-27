
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
#include <amelie_cdc.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>

void
wal_init(Wal* self)
{
	self->current          = NULL;
	self->files            = NULL;
	self->files_count      = 0;
	self->slots_count      = 0;
	self->subscribes_count = 0;
	self->dirfd            = -1;
	list_init(&self->slots);
	list_init(&self->subscribes);
	spinlock_init(&self->lock);
}

void
wal_free(Wal* self)
{
	assert(! self->current);
	spinlock_free(&self->lock);
}

void
wal_open(Wal* self)
{
	// open or create directory and add existing files
	wal_recovery(self);
}

void
wal_close(Wal* self)
{
	// close all files
	auto sync_on_close = opt_int_of(&config()->wal_sync_close);
	auto file = self->files;
	while (file)
	{
		auto next = file->next;
		error_catch
		(
			if (sync_on_close)
				wal_file_sync(file);
			wal_file_close(file);
		);
		wal_file_free(file);
		file = next;
	}
	self->files       = NULL;
	self->files_count = 0;
	self->current     = NULL;

	// close wal dir
	if (self->dirfd != -1)
	{
		vfs_close(self->dirfd);
		self->dirfd = -1;
	}
}

void
wal_gc(Wal* self, uint64_t min)
{
	// set min between the provided min and slots
	wal_slots(self, &min);

	// collect wal files < min (current file always remains)
	spinlock_lock(&self->lock);
	assert(self->current);

	// [1, 2, 3]
	WalFile* list = NULL;
	WalFile* head = self->files;
	while (head->next)
	{
		auto next = head->next;
		if (min < next->id)
			break;
		head->next = list;
		list = head;
		self->files_count--;
		head = next;
	}
	self->files = head;
	assert(head);

	spinlock_unlock(&self->lock);
	if (! list)
		return;

	// remove collected wal files
	uint64_t total = 0;
	auto     total_count = 0;
	auto     file = list;
	while (file)
	{
		auto next = file->next;
		auto size = file->file.size;
		if (wal_file_unpin(file))
		{
			total += size;
			total_count++;
		}
		file = next;
	}
	info("wal: %d files removed (%.2f MiB)", total_count,
	     (double)total / 1024 / 1024);
}

WalFile*
wal_find(Wal* self, uint64_t lsn, bool next_after)
{
	WalFile* match = NULL;
	spinlock_lock(&self->lock);

	// find wal file
	switch (lsn) {
	case 0:
		// min
		match = self->files;
		assert(match);
		break;
	case UINT64_MAX:
		// max (current)
		match = self->current;
		assert(match);
		break;
	default:
	{
		// find max file.id <= lsn < file_next.id
		match = self->files;
		while (match->next)
		{
			if (match->next->id > lsn)
				break;
			match = match->next;
		}
		break;
	}
	}

	if (match && next_after)
		match = match->next;

	if (match)
		wal_file_pin(match);

	spinlock_unlock(&self->lock);
	return match;
}

void
wal_status(Wal* self, Buf* buf)
{
	// get min file id and total count
	spinlock_lock(&self->lock);
	auto list_min   = UINT64_MAX;
	auto list_count = 0;
	for (auto file = self->files; file; file = file->next)
	{
		if (file->id < list_min)
			list_min = file->id;
	}
	list_count = self->files_count;
	spinlock_unlock(&self->lock);

	// get min slot and total count
	uint64_t slots_min = UINT64_MAX;
	int      slots_count;
	slots_count = wal_slots(self, &slots_min);

	// obj
	encode_obj(buf);

	// lsn
	encode_raw(buf, "lsn", 3);
	encode_int(buf, state_lsn());

	// lsn_min
	encode_raw(buf, "lsn_min", 7);
	encode_int(buf, list_min);

	// files
	encode_raw(buf, "files", 5);
	encode_int(buf, list_count);

	// slots
	encode_raw(buf, "slots", 5);
	encode_int(buf, slots_count);

	// slots_min
	encode_raw(buf, "slots_min", 9);
	encode_int(buf, slots_min);

	// writes
	encode_raw(buf, "writes", 6);
	encode_int(buf, opt_int_of(&state()->writes));

	// writes_bytes
	encode_raw(buf, "writes_bytes", 12);
	encode_int(buf, opt_int_of(&state()->writes_bytes));

	// ops
	encode_raw(buf, "ops", 3);
	encode_int(buf, opt_int_of(&state()->ops));

	encode_obj_end(buf);
}
