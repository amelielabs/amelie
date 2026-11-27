
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
#include <amelie_heap>
#include <amelie_wal.h>

static void
wal_sync_dir(Wal* self)
{
	auto rc = vfs_fsync(self->dirfd);
	if (rc == -1)
		error_system();
}

static void
wal_create(Wal* self, WalContext* context, uint64_t id)
{
	// create new wal file
	auto file = wal_file_allocate(id);
	errdefer(wal_file_free, file);
	wal_file_create(file);

	// add to the list and set as current
	spinlock_lock(&self->lock);

	auto prev = self->current;
	prev->next = file;
	self->current = file;
	self->files_count++;
	auto count = self->files_count;

	wal_file_pin(prev);
	wal_file_pin(file);

	spinlock_unlock(&self->lock);
	defer(wal_file_unpin_defer, prev);

	// sync and close previous file
	auto service = config()->wal_service;
	auto close = true;
	if (config()->wal_sync_close)
	{
		if (service) {
			context->sync_close = prev->id;
			close = false;
		} else {
			wal_file_sync(prev);
		}
	}
	if (close)
		wal_file_close(prev);

	// sync new file
	if (config()->wal_sync_create)
	{
		if (service) {
			context->sync = id;
		} else
		{
			wal_file_sync(file);
			wal_sync_dir(self);
		}
	}

	// schedule checkpoint on reaching files threshold
	if (count >= (int)config()->wal_checkpoint)
		context->checkpoint = true;
}

hot static inline void
wal_write_recover(WalFile* current, WalContext* context)
{
	// write
	wal_file_write(current, context->iov, context->iov_count);

	// sync after write
	if (config()->wal_sync_write)
		wal_file_sync(current);
}

hot static inline void
wal_write_list(WalFile* current, WalContext* context)
{
	context->lsn = state_lsn();

	// set lsn
	auto iov = context->iov;
	auto iov_count = context->iov_count;
	for (auto i = 0; i < iov_count; i++)
	{
		auto ptr = (uint8_t*)iov[i].iov_base;
		auto end = (uint8_t*)iov[i].iov_base + iov[i].iov_len;
		while (ptr < end)
		{
			auto record = (Record*)ptr;
			record_seal(record, ++context->lsn);
			ptr += record->size;
		}
	}

	// write
	wal_file_write(current, iov, iov_count);

	// sync after write
	if (config()->wal_sync_write)
		wal_file_sync(current);
}

hot void
wal_write(Wal* self, WalContext* context)
{
	// get or create a new wal file
	spinlock_lock(&self->lock);
	auto current = self->current;
	if (current->file.size >= (uint64_t)config()->wal_size)
	{
		spinlock_unlock(&self->lock);

		// create a new wal file
		wal_create(self, context, state_lsn() + 1);

		spinlock_lock(&self->lock);
		current = self->current;
	}
	wal_file_pin(current);
	spinlock_unlock(&self->lock);

	defer(wal_file_unpin_defer, current);

	// do atomical write of a list of wal records
	auto on_error = error_catch
	(
		if (context->recover)
			wal_write_recover(current, context);
		else
			wal_write_list(current, context);
	);
	if (unlikely(on_error))
		panic("wal: wal write failed");

	// set global lsn
	state_lsn_set(context->lsn);

	// notify subscribers
	wal_subscribe_notify(self, context->lsn);
}
