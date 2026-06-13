
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
wal_flush(Wal* self, WalFile* current, WalContext* context)
{
	auto iov = &self->iov;
	iov_reset(iov);

	context->lsn = state_lsn() + 1;
	list_foreach(&context->list->list)
	{
		auto write = list_at(Write, link);

		// finilize wal record
		write_set_lsn(write, context->lsn);
		write_seal(write);

		// [record][context, sql]
		iov_add(iov, &write->record, sizeof(write->record));
		iov_add_buf(iov, &write->record_data);

		if (! list_is_last(&context->list->list, &write->link))
			context->lsn++;
	}

	// write
	wal_file_write(current, iov);

	// sync after write
	if (opt_int_of(&config()->wal_sync_write))
		wal_file_sync(current);
}

hot static inline void
wal_recover(WalContext* context)
{
	context->lsn = state_lsn();
	list_foreach(&context->list->list)
	{
		auto write = list_at(Write, link);
		assert(write->recover);
		context->lsn = write->recover->lsn;
	}
}

hot static inline void
wal_recover_write(Wal* self, WalFile* current, WalContext* context)
{
	auto iov = &self->iov;
	iov_reset(iov);

	context->lsn = state_lsn();
	list_foreach(&context->list->list)
	{
		auto write = list_at(Write, link);
		assert(write->recover);
		context->lsn = write->recover->lsn;
		iov_add(iov, write->recover, write->recover->size);
	}

	// write
	wal_file_write(current, iov);

	// sync after write
	if (opt_int_of(&config()->wal_sync_write))
		wal_file_sync(current);
}

hot static inline void
wal_write_state(Wal* self, WalFile* current, WalContext* context)
{
	// handle current system recovery state
	switch (opt_int_of(&state()->recover)) {
	case RECOVER_OFF:
		// wal write
		wal_flush(self, current, context);
		break;
	case RECOVER_WAL:
		// wal recover
		wal_recover(context);
		break;
	case RECOVER_REPL:
		// wal recover (with write)
		wal_recover_write(self, current, context);
		break;
	}

	context->list->lsn = context->lsn;
}

hot void
wal_write(Wal* self, WalContext* context)
{
	auto recover_state = opt_int_of(&state()->recover);
	auto do_write =
		recover_state == RECOVER_OFF ||
		recover_state == RECOVER_REPL;

	// get or create a new wal file
	spinlock_lock(&self->lock);
	auto current = self->current;
	if (do_write && (current->file.size >= opt_int_of(&config()->wal_size)))
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
	auto on_error = error_catch
	(
		wal_write_state(self, current, context)
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
