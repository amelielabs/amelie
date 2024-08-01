
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
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>

static inline void
set_free_row(Set* self, SetRow* row)
{
	value_free(&row->value);
	int i = 0;
	for (; i < self->keys_count ; i++)
		value_free(&row->keys[i]);
	am_free(row);
}

static void
set_free(ValueObj* obj)
{
	auto self = (Set*)obj;
	for (int i = 0; i < self->list_count ; i++)
		set_free_row(self, set_at(self, i));
	buf_free(&self->list);
	if (self->keys)
		am_free(self->keys);
	am_free(self);
}

static void
set_encode(ValueObj* obj, Buf* buf)
{
	auto self = (Set*)obj;
	encode_array(buf);
	int i = 0;
	for (; i < self->list_count ; i++)
		value_write(&set_at(self, i)->value, buf);
	encode_array_end(buf);
}

static void
set_decode(ValueObj* obj, Buf* buf, Timezone* timezone)
{
	auto self = (Set*)obj;
	int i = 0;
	for (; i < self->list_count ; i++)
	{
		if (i > 0)
			body_add_comma(buf);
		body_add(buf, &set_at(self, i)->value, timezone, true);
	}
}

hot static bool
set_in(ValueObj* obj, Value* value)
{
	auto self = (Set*)obj;
	int i = 0;
	for (; i < self->list_count ; i++)
	{
		auto at = set_at(self, i);
		if (value_is_equal(&at->value, value))
			return true;
	}
	return false;
}

Set*
set_create(uint8_t* data)
{
	Set* self = am_malloc(sizeof(Set));
	self->obj.free   = set_free;
	self->obj.encode = set_encode;
	self->obj.decode = set_decode;
	self->obj.in     = set_in;
	self->list_count = 0;
	self->keys       = NULL;
	self->keys_count = 0;
	buf_init(&self->list);
	guard(set_free, self);

	if (data)
	{
		// [asc/desc, ...]
		self->keys_count = array_size(data);
		data_read_array(&data);
		if (self->keys_count > 0)
		{
			self->keys = am_malloc(sizeof(SetKey) * self->keys_count);
			for (int i = 0; i < self->keys_count; i++)
				data_read_bool(&data, &self->keys[i].asc);
		}
	}
	return unguard();
}

hot void
set_add(Set* self, Value* value, Value** keys)
{
	buf_reserve(&self->list, sizeof(SetRow**));
	int size = sizeof(SetRow) + sizeof(Value) * self->keys_count;

	SetRow* row = am_malloc(size);
	memset(row, 0, size);
	buf_write(&self->list, &row, sizeof(SetRow**));
	self->list_count++;

	value_copy(&row->value, value);
	for (int i = 0; i < self->keys_count; i++)
		value_copy(&row->keys[i], keys[i]);
}

hot void
set_add_from_stack(Set* self, Value* value, Stack* stack)
{
	if (self->keys == 0)
	{
		set_add(self, value, NULL);
		return;
	}
	Value* keys[self->keys_count];
	for (int i = 0; i < self->keys_count; i++)
	{
		auto value = stack_at(stack, self->keys_count - i);
		keys[i] = value;
	}
	set_add(self, value, keys);
}

hot static int
set_cmp(const void* p1, const void* p2, void* arg)
{
	Set* self = arg;
	return set_compare(self->keys, self->keys_count,
	                   *(SetRow**)p1,
	                   *(SetRow**)p2);
}

void
set_sort(Set* self)
{
	qsort_r(self->list.start, self->list_count, sizeof(SetRow**), set_cmp, self);
}
