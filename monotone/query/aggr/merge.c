
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
#include <monotone_index.h>
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

static void
merge_convert(ValueObj* obj, Buf* buf)
{
	auto self  = (Merge*)obj;
	int  count = 0;
	auto list  = (SetIterator*)self->list.start;
	for (int i = 0; i < self->list_count; i++)
	{
		auto set = list[i].set;
		count += set->list_count;
	}

	encode_array(buf, count);
	while (merge_has(self))
	{
		value_write(&merge_at(self)->value, buf);
		merge_next(self);
	}
}

Merge*
merge_create(void)
{
	Merge* self = mn_malloc(sizeof(Merge));
	self->obj.free    = merge_free;
	self->obj.convert = merge_convert;
	self->current_it  = NULL;
	self->current     = NULL;
	self->keys        = NULL;
	self->keys_count  = 0;
	self->list_count  = 0;
	self->limit       = INT64_MAX;
	self->distinct    = false;
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

hot static inline void
merge_step(Merge* self)
{
	auto list = (SetIterator*)self->list.start;
	if (self->current_it)
	{
		set_iterator_next(self->current_it);
		self->current_it = NULL;
	}
	self->current = NULL;

	SetIterator* min_iterator = NULL;
	SetRow*      min = NULL;
	for (int pos = 0; pos < self->list_count; pos++)
	{
		auto current = &list[pos];
		auto row = set_iterator_at(current);
		if (row == NULL)
			continue;

		if (min == NULL)
		{
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
			min_iterator = current;
			min = row;
			break;
		case -1:
			break;
		}
	}
	self->current_it = min_iterator;
	self->current    = min;
}

hot void
merge_next(Merge* self)
{
	// apply limit
	if (self->limit-- <= 0)
	{
		self->current_it = NULL;
		self->current    = NULL;
		return;
	}

	if (! self->distinct)
	{
		merge_step(self);
		return;
	}

	// skip duplicates
	auto prev = self->current;
	for (;;)
	{
		merge_step(self);
		auto at = merge_at(self);
		if (unlikely(!at || !prev))
			break;
		if (set_compare(self->keys, self->keys_count, prev, at) != 0)
			break;
	}
}

void
merge_open(Merge* self, bool distinct, int64_t limit, int64_t offset)
{
	self->distinct = distinct;

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
