
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>

static void
merge_free(ValueObj* obj)
{
	auto self = (Merge*)obj;
	auto list = (Set**)self->list.start;
	for (int i = 0; i < self->list_count; i++)
	{
		auto set = list[i];
		set->obj.free(&set->obj);
	}
	buf_free(&self->list);
	so_free(self);
}

static void
merge_encode(ValueObj* obj, Buf* buf)
{
	auto self = (Merge*)obj;
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);

	// []
	int start = buf_size(buf);
	encode_array32(buf, 0);

	merge_iterator_open(&it, self);
	int count = 0;
	while (merge_iterator_has(&it))
	{
		auto row = merge_iterator_at(&it);
		value_write(&row->value, buf);
		merge_iterator_next(&it);
		count++;
	}

	// update pos
	uint8_t* pos = buf->start + start;
	data_write_array32(&pos, count);
}

static void
merge_decode(ValueObj* obj, Body* body)
{
	auto self = (Merge*)obj;
	MergeIterator it;
	merge_iterator_init(&it);
	guard(merge_iterator_free, &it);

	merge_iterator_open(&it, self);
	auto first = true;
	while (merge_iterator_has(&it))
	{
		auto row = merge_iterator_at(&it);
		if (! first)
			body_add_comma(body);
		else
			first = false;
		body_add(body, &row->value);
		merge_iterator_next(&it);
	}
}

Merge*
merge_create(void)
{
	Merge* self = so_malloc(sizeof(Merge));
	self->obj.free   = merge_free;
	self->obj.encode = merge_encode;
	self->obj.decode = merge_decode;
	self->keys       = NULL;
	self->keys_count = 0;
	self->list_count = 0;
	self->limit      = INT64_MAX;
	self->offset     = 0;
	self->distinct   = false;
	buf_init(&self->list);
	return self;
}

void
merge_add(Merge* self, Set* set)
{
	// add set iterator
	buf_write(&self->list, &set, sizeof(Set**));
	self->list_count++;

	// set keys
	if (self->keys == NULL)
	{
		self->keys = set->keys;
		self->keys_count = set->keys_count;
	}
}

void
merge_open(Merge* self, bool distinct, int64_t limit, int64_t offset)
{
	self->distinct = distinct;
	self->limit    = limit;
	self->offset   = offset;
}
