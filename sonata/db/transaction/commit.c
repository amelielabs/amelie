
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>

void
transaction_begin(Transaction* self)
{
	if (unlikely(transaction_active(self)))
		error("transaction is active");
	self->active  = true;
	self->aborted = false;
	self->arg     = NULL;
}

static inline void
transaction_end(Transaction* self, bool aborted)
{
	// reset log
	log_reset(&self->log);

	// done
	self->active  = false;
	self->aborted = aborted;
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
		op->commit(op);
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
