
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
handle_mgr_create_commit(LogOp* op)
{
	buf_free(op->handle.data);
}

static void
handle_mgr_create_abort(LogOp* op)
{
	HandleMgr* self = op->arg;
	handle_mgr_delete(self, op->handle.handle);
	handle_free(op->handle.handle);
	buf_free(op->handle.data);
}

void
handle_mgr_create(HandleMgr*   self,
                  Transaction* trx,
                  LogCmd       cmd,
                  Handle*      handle,
                  Buf*         data)
{
	log_reserve(&trx->log);

	// update handle mgr
	handle_mgr_set(self, handle);

	// update transaction log
	log_handle(&trx->log, cmd,
	           handle_mgr_create_commit,
	           handle_mgr_create_abort, self,
	           handle, data);
}

static void
handle_mgr_drop_commit(LogOp* op)
{
	Handle* handle = op->handle.handle;
	handle_free(handle);
	buf_free(op->handle.data);
}

static void
handle_mgr_drop_abort(LogOp* op)
{
	HandleMgr* self = op->arg;
	handle_mgr_set(self, op->handle.handle);
	buf_free(op->handle.data);
}

void
handle_mgr_drop(HandleMgr*   self,
                Transaction* trx,
                LogCmd       cmd,
                Handle*      handle,
                Buf*         data)
{
	log_reserve(&trx->log);

	// update handle mgr
	handle_mgr_delete(self, handle);

	// update transaction log
	log_handle(&trx->log, cmd,
	           handle_mgr_drop_commit,
	           handle_mgr_drop_abort, self,
	           handle, data);
}
