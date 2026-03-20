
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
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
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
	auto service = opt_int_of(&config()->wal_service);
	auto close = true;
	if (opt_int_of(&config()->wal_sync_close))
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
	if (opt_int_of(&config()->wal_sync_create))
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
	if (count >= (int)opt_int_of(&config()->wal_checkpoint))
		context->checkpoint = true;
}

hot static inline void
wal_flush(WalFile* current, WalContext* context)
{
	context->lsn = state_lsn() + 1;
	list_foreach(&context->list->list)
	{
		auto write = list_at(Write, link);

		// update stats
		opt_int_add(&state()->writes, 1);
		opt_int_add(&state()->writes_bytes, write->header.size);
		opt_int_add(&state()->ops, write->header.ops);

		// finilize wal record
		write_end(write, context->lsn);

		// write wal file

		// [header][commands][rows or ops]
		wal_file_write(current, &write->iov);
		list_foreach(&write->list)
		{
			auto write_log = list_at(WriteLog, link);
			wal_file_write(current, &write_log->iov);
		}

		if (! list_is_last(&context->list->list, &write->link))
			context->lsn++;
	}
	context->list->lsn = context->lsn;

	// sync after write
	if (opt_int_of(&config()->wal_sync_write))
		wal_file_sync(current);
}

hot void
wal_write(Wal* self, WalContext* context)
{
	// note: assuming only one wal writer in the system

	// get or create a new wal file
	spinlock_lock(&self->lock);
	auto current = self->current;
	if (current->file.size >= opt_int_of(&config()->wal_size))
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
	auto current_offset = current->file.size;
	auto on_error = error_catch (
		wal_flush(current, context)
	);
	if (unlikely(on_error)) {
		if (error_catch(wal_file_truncate(current, current_offset)))
			panic("wal: failed to truncate wal file after failed write");
		rethrow();
	}

	// set global lsn
	state_lsn_set(context->lsn);

	// notify subscribers
	wal_subscribe_notify(self, context->lsn);
}
