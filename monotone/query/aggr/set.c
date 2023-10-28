
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

static inline void
set_free_row(Set* self, SetRow* row)
{
	value_free(&row->value);
	int i = 0;
	for (; i < self->keys_count ; i++)
		value_free(&row->keys[i]);
	mn_free(row);
}

static void
set_free(ValueObj* obj)
{
	auto self = (Set*)obj;
	for (int i = 0; i < self->list_count ; i++)
		set_free_row(self, set_at(self, i));
	buf_free(&self->list);
	if (self->keys)
		mn_free(self->keys);
	mn_free(self);
}

static void
set_convert(ValueObj* obj, Buf* buf)
{
	auto self = (Set*)obj;
	encode_array(buf, self->list_count);
	int i = 0;
	for (; i < self->list_count ; i++)
		value_write(&set_at(self, i)->value, buf);
}

Set*
set_create(uint8_t* data)
{
	Set* self = mn_malloc(sizeof(Set));
	self->obj.free    = set_free;
	self->obj.convert = set_convert;
	self->list_count  = 0;
	self->keys        = NULL;
	self->keys_count  = 0;
	buf_init(&self->list);
	guard(self_guard, set_free, self);

	if (data)
	{
		// [[type, asc], ...]
		data_read_array(&data, &self->keys_count);
		if (self->keys_count > 0)
		{
			self->keys = mn_malloc(sizeof(SetKey) * self->keys_count);
			for (int i = 0; i < self->keys_count; i++)
			{
				int count;
				data_read_array(&data, &count);
				data_read_integer(&data, &self->keys[i].type);
				data_read_bool(&data, &self->keys[i].asc);
			}
		}
	}

	return unguard(&self_guard);
}

hot void
set_add(Set* self, Value* value, Value** keys)
{
	buf_reserve(&self->list, sizeof(SetRow**));
	int size = sizeof(SetRow) + sizeof(Value) * self->keys_count;

	SetRow* row = mn_malloc(size);
	memset(row, 0, size);
	guard(row_guard, set_free_row, row);

	value_copy(&row->value, value);
	for (int i = 0; i < self->keys_count; i++)
		value_copy(&row->keys[i], keys[i]);
	buf_write(&self->list, &row, sizeof(SetRow**));
	self->list_count++;

	unguard(&row_guard);
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
		if (self->keys[i].type == TYPE_STRING)
		{
			if (value->type != VALUE_STRING)
				error("ORDER BY expression <%d>: unexpected data type (string expected)", i);
		} else
		{
			if (value->type != VALUE_INT)
				error("ORDER BY expression <%d>: unexpected data type (int expected)", i);
		}
		keys[i] = value;
	}
	set_add(self, value, keys);
}

hot static inline int
set_compare(Set* self, SetRow* a, SetRow* b)
{
	for (int i = 0; i < self->keys_count; i++)
	{
		auto key = &self->keys[i];
		int rc;
		if (key->type == TYPE_STRING)
			rc = value_compare_string(&a->keys[i], &b->keys[i]);
		else
			rc = value_compare_int(&a->keys[i], &b->keys[i]);
		if (rc != 0)
			return (key->asc) ? rc : -rc;
	}
	return 0;
}

hot static int
set_cmp(const void* p1, const void* p2, void* arg)
{
	return set_compare(arg, *(SetRow**)p1, *(SetRow**)p2);
}

void
set_sort(Set* self)
{
	qsort_r(self->list.start, self->list_count, sizeof(SetRow**), set_cmp, self);
}
