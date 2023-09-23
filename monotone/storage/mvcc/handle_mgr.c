
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
handle_mgr_init(HandleMgr* self)
{
	self->list_count    = 0;
	self->free_function = NULL;
	self->free_arg      = NULL;
	list_init(&self->list);
}

void
handle_mgr_free(HandleMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto handle = list_at(Handle, link);
		while (handle)
		{
			auto prev = handle->prev;
			handle_free(handle, self);
			handle = prev;
		}
	}
	self->list_count = 0;
	list_init(&self->list);
}

void
handle_mgr_set_free(HandleMgr*       self,
                    HandleDestructor free_function,
                    void*            free_arg)
{
	self->free_function = free_function;
	self->free_arg      = free_arg;
}

static inline void
handle_mgr_gc_of(HandleMgr* self, Handle* handle, uint64_t snapshot_min)
{
	// [head]
	// [head] - [0]
	// [head] - [1] - [0]

	// keep at least one record < snapshot_min 
	uint64_t last_commited_lsn = UINT64_MAX;
	while (handle)
	{
		if (! handle_is_commited(handle))
		{
			handle = handle->prev;
			continue;
		}

		// remove same transaction updates (keep only the latest)
		if (handle->lsn == last_commited_lsn)
		{
			auto prev = handle->prev;
			handle_mgr_unset(self, handle);
			handle = prev;
			continue;
		}
		last_commited_lsn = handle->lsn;

		if (handle->lsn >= snapshot_min)
		{
			handle = handle->prev;
			continue;
		}

		// remove if more recent version exists
		// remove if delete
		// remove if all older versions, if this is the only one

		// last version < snapshot_min (can be delete)

		// keep at least one version, unless delete
		if (! handle->drop)
			handle = handle->prev;
		while (handle)
		{
			auto prev = handle->prev;
			handle_mgr_unset(self, handle);
			handle = prev;
		}
		break;
	}
}

void
handle_mgr_gc(HandleMgr* self, uint64_t snapshot_min)
{
	list_foreach_safe(&self->list)
	{
		auto head = list_at(Handle, link);
		handle_mgr_gc_of(self, head, snapshot_min);
	}
}

int
handle_mgr_count(HandleMgr* self)
{
	int count = 0;
	list_foreach(&self->list)
	{
		auto handle = list_at(Handle, link);
		if (handle->drop)
			continue;
		count++;
	}
	return count;
}

static inline Handle*
handle_mgr_find_head(HandleMgr* self, Str* name)
{
	list_foreach(&self->list) {
		auto handle = list_at(Handle, link);
		if (str_compare(handle->name, name))
			return handle;
	}
	return NULL;
}

void
handle_mgr_set(HandleMgr* self, Handle* handle)
{
	// match head
	auto head = handle_mgr_find_head(self, handle->name);

	// update list
	if (head)
	{
		handle_link(head, handle);
		list_unlink(&head->link);
		list_append(&self->list, &handle->link);
	} else {
		list_append(&self->list, &handle->link);
	}
	self->list_count++;
}

void
handle_mgr_unset(HandleMgr* self, Handle* handle)
{
	// replace to next or remove from handle_mgr
	if (handle->next == NULL)
	{
		list_unlink(&handle->link);
		if (handle->prev)
			list_append(&self->list, &handle->prev->link);
	}
	handle_unlink(handle);
	handle_free(handle, self);

	self->list_count--;
	assert(self->list_count >= 0);
}

Handle*
handle_mgr_get(HandleMgr* self, Str* name)
{
	auto head = handle_mgr_find_head(self, name);
	if (head && head->drop)
		return NULL;
	return head;
}
