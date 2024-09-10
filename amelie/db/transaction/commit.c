
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>

void
tr_begin(Tr* self)
{
	if (unlikely(tr_active(self)))
		error("transaction is active");
	self->active  = true;
	self->aborted = false;
	self->arg     = NULL;
}

static inline void
tr_end(Tr* self, bool aborted)
{
	// reset log
	log_reset(&self->log);

	// done
	self->active  = false;
	self->aborted = aborted;
}

hot void
tr_commit(Tr* self)
{
	if (unlikely(! tr_active(self)))
		error("transaction is not active");

	auto log = &self->log;
	for (int pos = 0; pos < log->count; pos++)
	{
		auto op = log_of(log, pos);
		op->iface->commit(log, op);
	}

	tr_end(self, false);
}

void
tr_abort(Tr* self)
{
	if (unlikely(! tr_active(self)))
		error("transaction is not active");

	auto log = &self->log;
	for (int pos = self->log.count - 1; pos >= 0; pos--)
	{
		auto op = log_of(log, pos);
		op->iface->abort(log, op);
	}

	tr_end(self, true);
}
