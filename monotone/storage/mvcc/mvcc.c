
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>

void
mvcc_begin(Transaction* self)
{
	if (unlikely(transaction_active(self)))
		error("transaction is active");
	self->lsn         = 0;
	self->active      = true;
	self->aborted     = false;
	self->auto_commit = false;
	self->arg         = NULL;
}

static inline void
mvcc_end(Transaction* self, bool aborted)
{
	// reset log
	log_reset(&self->log);

	// done
	self->active      = false;
	self->aborted     = aborted;
	self->auto_commit = false;
}

hot void
mvcc_commit(Transaction* self)
{
	if (unlikely(! transaction_active(self)))
		error("transaction is not active");

	for (int pos = 0; pos < self->log.count; pos++)
	{
		auto op = log_of(&self->log, pos);
		switch (op->type) {
		case LOG_WRITE:
		{	
			// add row to the commit list and update commit lsn
			// free previous row
			heap_commit(op->write.heap, op->write.row, op->write.prev, self->lsn);
			lock_unlock(op->write.locker);
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			// set lsn and free previous handle
			handle_cache_commit(op->write_handle.handle_cache,
			                    op->write_handle.handle,
			                    op->write_handle.handle_prev,
			                    self->lsn);
			if (op->write_handle.data)
				buf_free(op->write_handle.data);
			break;
		}
		}
	}

	mvcc_end(self, false);
}

void
mvcc_abort(Transaction* self)
{
	if (unlikely(! transaction_active(self)))
		error("transaction is not active");

	int pos;
	pos = self->log.count - 1;
	for (; pos >= 0; pos--)
	{
		auto op = log_of(&self->log, pos);

		switch (op->type) {
		case LOG_WRITE:
		{	
			// remove operation from the heap
			heap_abort(op->write.heap, op->write.row, op->write.prev);
			lock_unlock(op->write.locker);
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			// remove handle from the cache and bring previous one
			handle_cache_abort(op->write_handle.handle_cache,
			                   op->write_handle.handle,
			                   op->write_handle.handle_prev);
			if (op->write_handle.data)
				buf_free(op->write_handle.data);
			break;
		}
		}

		log_delete_last(&self->log);
	}

	mvcc_end(self, true);
}

hot bool
mvcc_write(Transaction* self,
           LogCmd       cmd,
           Heap*        heap,
           Locker*      locker,
           Row*         row)
{
	log_reserve(&self->log);

	// update heap
	Row* prev = NULL;
	if (cmd == LOG_WRITE)
		prev = heap_set(heap, row);
	else
	if (cmd == LOG_DELETE)
		prev = heap_delete(heap, row);

	// update transaction log
	log_add_write(&self->log, cmd, heap, locker, row, prev);

	// is update unique key
	bool update;
	update = cmd == LOG_WRITE && prev;
	return update;
}

void
mvcc_write_handle(Transaction* self,
                  LogCmd       cmd,
                  HandleCache* handle_cache,
                  Handle*      handle,
                  Buf*         data)
{
	log_reserve(&self->log);

	// update handle cache
	Handle* prev = NULL;
	if (cmd == LOG_CREATE_TABLE ||
	    cmd == LOG_ALTER_TABLE  ||
	    cmd == LOG_CREATE_META)
	{
		prev = handle_cache_set(handle_cache, handle);
	} else
	{
		prev = handle_cache_delete(handle_cache, handle->name);
	}

	// update transaction log
	log_add_write_handle(&self->log, cmd, handle_cache, handle, prev, data);
}
