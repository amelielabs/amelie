
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
#include <monotone_key.h>
#include <monotone_transaction.h>

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

Handle*
handle_mgr_set(HandleMgr* self, Handle* handle)
{
	auto prev = handle_mgr_delete(self, handle->name);
	list_append(&self->list, &handle->link);
	self->list_count++;
	return prev;
}

Handle*
handle_mgr_delete(HandleMgr* self, Str* name)
{
	auto prev = handle_mgr_get(self, name);
	if (prev)
	{
		list_unlink(&prev->link);
		list_init(&prev->link);
		self->list_count--;
	}
	return prev;
}

Handle*
handle_mgr_get(HandleMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto handle = list_at(Handle, link);
		if (str_compare(handle->name, name))
			return handle;
	}
	return NULL;
}

void
handle_mgr_abort(HandleMgr* self, Handle* handle, Handle* prev)
{
	if (handle)
	{
		handle_mgr_delete(self, handle->name);
		handle_free(handle);
	}

	if (prev)
		handle_mgr_set(self, prev);
}

void
handle_mgr_commit(HandleMgr* self, Handle* handle, Handle* prev, uint64_t lsn)
{
	unused(self);
	if (handle)
		handle->lsn = lsn;
	if (prev)
		handle_free(prev);
}

void
handle_mgr_write(HandleMgr*   self,
                 Transaction* trx,
                 LogCmd       cmd,
                 Handle*      handle,
                 Buf*         data)
{
	log_reserve(&trx->log, cmd, NULL);

	// update handle mgr
	Handle* prev = NULL;
	if (cmd == LOG_CREATE_TABLE ||
	    cmd == LOG_ALTER_TABLE  ||
	    cmd == LOG_CREATE_META)
		prev = handle_mgr_set(self, handle);
	else
		prev = handle_mgr_delete(self, handle->name);

	// update transaction log
	log_add_handle(&trx->log, cmd, self, handle, prev, data);
}
