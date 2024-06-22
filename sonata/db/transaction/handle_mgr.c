
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
#include <sonata_row.h>
#include <sonata_transaction.h>

void
handle_mgr_init(HandleMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
handle_mgr_free(HandleMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto handle = list_at(Handle, link);
		handle_free(handle);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
handle_mgr_set(HandleMgr* self, Handle* handle)
{
	// previous version should not exists
	list_append(&self->list, &handle->link);
	self->list_count++;
}

static inline void
handle_mgr_delete(HandleMgr* self, Handle* handle)
{
	list_unlink(&handle->link);
	list_init(&handle->link);
	self->list_count--;
}

Handle*
handle_mgr_get(HandleMgr* self, Str* schema, Str* name)
{
	list_foreach(&self->list)
	{
		auto handle = list_at(Handle, link);
		if (schema && !str_compare(handle->schema, schema))
			continue;
		if (str_compare(handle->name, name))
			return handle;
	}
	return NULL;
}

static void
create_if_commit(Log* self, LogOp* op)
{
	buf_free(log_handle_of(self, op)->data);
}

static void
create_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	HandleMgr* mgr = op->iface_arg;
	handle_mgr_delete(mgr, handle->handle);
	handle_free(handle->handle);
	buf_free(handle->data);
}

static LogIf create_if =
{
	.commit = create_if_commit,
	.abort  = create_if_abort
};

void
handle_mgr_create(HandleMgr*   self,
                  Transaction* trx,
                  LogCmd       cmd,
                  Handle*      handle,
                  Buf*         data)
{
	// update handle mgr
	handle_mgr_set(self, handle);

	// update transaction log
	log_handle(&trx->log, cmd, &create_if, self, handle, data);
}

static void
drop_if_commit(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	handle_free(handle->handle);
	buf_free(handle->data);
}

static void
drop_if_abort(Log* self, LogOp* op)
{
	auto handle = log_handle_of(self, op);
	HandleMgr* mgr = op->iface_arg;
	handle_mgr_set(mgr, handle->handle);
	buf_free(handle->data);
}

static LogIf drop_if =
{
	.commit = drop_if_commit,
	.abort  = drop_if_abort
};

void
handle_mgr_drop(HandleMgr*   self,
                Transaction* trx,
                LogCmd       cmd,
                Handle*      handle,
                Buf*         data)
{
	// update handle mgr
	handle_mgr_delete(self, handle);

	// update transaction log
	log_handle(&trx->log, cmd, &drop_if, self, handle, data);
}
