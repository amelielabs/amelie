
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

	for (int pos = 0; pos < self->log.count; pos++)
	{
		auto op = log_of(&self->log, pos);
		switch (op->type) {
		case LOG_WRITE:
		{	
			// free previous version and set lsn
			op->write.iface->commit(op->write.iface_arg,
			                        op->write.row,
			                        op->write.prev,
			                        self->lsn);
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			// set lsn and free previous handle
			handle_mgr_commit(op->write_handle.handle_mgr,
			                  op->write_handle.handle,
			                  op->write_handle.handle_prev,
			                  self->lsn);
			if (op->write_handle.data)
				buf_free(op->write_handle.data);
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

	int pos;
	pos = self->log.count - 1;
	for (; pos >= 0; pos--)
	{
		auto op = log_of(&self->log, pos);

		switch (op->type) {
		case LOG_WRITE:
		{	
			// remove operation from the heap
			op->write.iface->abort(op->write.iface_arg,
			                       op->write.row,
			                       op->write.prev);
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			// remove handle from the mgr and bring previous one
			handle_mgr_abort(op->write_handle.handle_mgr,
			                 op->write_handle.handle,
			                 op->write_handle.handle_prev);
			if (op->write_handle.data)
				buf_free(op->write_handle.data);
			break;
		}
		}

		log_delete_last(&self->log);
	}

	transaction_end(self, true);
}
