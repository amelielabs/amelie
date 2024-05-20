
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>

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
	for (int pos = 0; pos < log->count; pos++)
	{
		auto op = log_of(log, pos);
		op->commit(op, self->lsn);
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
