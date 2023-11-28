
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
#include <monotone_def.h>
#include <monotone_transaction.h>

void
transaction_begin(Transaction* self)
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
transaction_end(Transaction* self, bool aborted)
{
	// reset log
	log_reset(&self->log);

	// done
	self->active      = false;
	self->aborted     = aborted;
	self->auto_commit = false;
}

hot void
transaction_commit(Transaction* self)
{
	if (unlikely(! transaction_active(self)))
		error("transaction is not active");

	auto log = &self->log;
	if (!log->count_prev && !log->count_handle)
	{
		transaction_end(self, false);
		return;
	}

	for (int pos = 0; pos < log->count; pos++)
	{
		auto op = log_of(log, pos);
		switch (op->type) {
		case LOG_WRITE:
		{
			if (op->row.prev)
				row_free(op->row.prev);
			break;
		}
		case LOG_CREATE:
		case LOG_ALTER:
		{
			Handle* handle = op->handle.handle;
			handle->lsn = self->lsn;
			buf_free(op->handle.data);
			break;
		}
		case LOG_DROP:
		{
			Handle* handle = op->handle.handle;
			handle_free(handle);
			buf_free(op->handle.data);
			break;
		}
		}
	}

	transaction_end(self, false);
}

void
transaction_abort(Transaction* self)
{
	if (unlikely(! transaction_active(self)))
		error("transaction is not active");

	int pos = self->log.count - 1;
	for (; pos >= 0; pos--)
	{
		auto op = log_of(&self->log, pos);
		op->abort(op);
	}

	transaction_end(self, true);
}
