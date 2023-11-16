
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
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>

static void
merge_free(ValueObj* obj)
{
	auto self = (Merge*)obj;
	auto list = (SetIterator*)self->list.start;
	for (int i = 0; i < self->list_count; i++)
	{
		auto set = list[i].set;
		set->obj.free(&set->obj);
	}
	buf_free(&self->list);
	mn_free(self);
}

Merge*
merge_create(void)
{
	Merge* self = mn_malloc(sizeof(Merge));
	self->obj.free    = merge_free;
	self->obj.convert = NULL;
	self->current     = NULL;
	self->keys        = NULL;
	self->keys_count  = 0;
	self->list_count  = 0;
	self->limit       = INT64_MAX;
	buf_init(&self->list);
	return self;
}

void
merge_add(Merge* self, Set* set)
{
	// add set iterator
	buf_reserve(&self->list, sizeof(SetIterator));
	auto it = (SetIterator*)self->list.position;
	set_iterator_init(it);
	set_iterator_open(it, set);
	buf_advance(&self->list, sizeof(SetIterator));
	self->list_count++;

	// set keys
	if (self->keys == NULL)
	{
		self->keys = set->keys;
		self->keys_count = set->keys_count;
	}
}

#if 0
hot void
merge_next(Merge* self)
{
	auto list = (SetIterator*)self->list.start;
	int dups  = 0;
	int pos   = 0;
	for (; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		if (current->advance)
		{
			set_iterator_next(current);
			current->advance = false;
		}
	}
	self->current = NULL;

	// apply limit
	if (self->limit-- <= 0)
		return;

	SetRow* min = NULL;
	for (pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = set_iterator_at(current);
		if (row == NULL)
			continue;

		if (min == NULL)
		{
			current->advance = true;
			min = row;
			if (! self->keys)
				break;
			continue;
		}

		int rc;
		rc = set_compare(self->keys, self->keys_count, min, row);
		switch (rc) {
		case 0:
			current->advance = true;
			dups++;
			break;
		case 1:
		{
			int i = 0;
			while (i < self->list_count)
			{
				list[i].advance = false;
				i++;
			}
			dups = 0;
			current->advance = true;
			min = row; 
			break;
		}
		}
	}
	self->current = min;
}
#endif

hot void
merge_next(Merge* self)
{
	auto list = (SetIterator*)self->list.start;
	int pos   = 0;
	for (; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		if (current->advance)
		{
			set_iterator_next(current);
			current->advance = false;
		}
	}
	self->current = NULL;

	// apply limit
	if (self->limit-- <= 0)
		return;

	SetIterator* min_iterator = NULL;
	SetRow*      min = NULL;
	for (pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = set_iterator_at(current);
		if (row == NULL)
			continue;

		if (min == NULL)
		{
			current->advance = true;
			min_iterator = current;
			min = row;
			if (! self->keys)
				break;
			continue;
		}

		int rc;
		rc = set_compare(self->keys, self->keys_count, min, row);
		switch (rc) {
		case 0:
			break;
		case 1:
			min_iterator->advance = false;
			current->advance = true;
			min_iterator = current;
			min = row; 
			break;
		case -1:
			break;
		}
	}
	self->current = min;
}

void
merge_open(Merge* self, int64_t limit, int64_t offset)
{
	// set to position
	if (offset == 0)
	{
		self->limit = limit;
		merge_next(self);
		return;
	}

	merge_next(self);

	// apply offset
	while (offset-- > 0 && merge_has(self))
		merge_next(self);

	self->limit = limit - 1;
	if (self->limit < 0)
		self->current = NULL;
}
