
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
handle_cache_init(HandleCache* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
handle_cache_free(HandleCache* self)
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
handle_cache_set(HandleCache* self, Handle* handle)
{
	auto prev = handle_cache_delete(self, handle->name);
	list_append(&self->list, &handle->link);
	self->list_count++;
	return prev;
}

Handle*
handle_cache_delete(HandleCache* self, Str* name)
{
	auto prev = handle_cache_get(self, name);
	if (prev)
	{
		list_unlink(&prev->link);
		list_init(&prev->link);
		self->list_count--;
	}
	return prev;
}

Handle*
handle_cache_get(HandleCache* self, Str* name)
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
handle_cache_abort(HandleCache* self, Handle* handle, Handle* prev)
{
	if (handle)
	{
		handle_cache_delete(self, handle->name);
		handle_free(handle);
	}

	if (prev)
		handle_cache_set(self, prev);
}

void
handle_cache_commit(HandleCache* self, Handle* handle, Handle* prev, uint64_t lsn)
{
	unused(self);

	if (handle)
		handle->lsn = lsn;

	if (prev)
		handle_free(prev);
}

void
handle_cache_sync(HandleCache* self, HandleCache* with)
{
	// remove all handles that are no longer exists
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(Handle, link);
		auto ref_with = handle_cache_get(with, ref->name);

		// delete if not exists or changed
		if (!ref_with || ref->lsn != ref_with->lsn)
		{
			list_unlink(&ref->link);
			self->list_count--;
			handle_free(ref);
		}
	}

	// add new
	list_foreach(&with->list)
	{
		auto ref = list_at(Handle, link);
		if (! handle_cache_get(self, ref->name))
		{
			auto new_handle = handle_copy(ref);
			list_append(&self->list, &new_handle->link);
			self->list_count++;
		}
	}

	assert(self->list_count == with->list_count);
}
