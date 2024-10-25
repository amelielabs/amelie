#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

hot static inline void
value_ref(Keys* self, Ref* row, Stack* stack)
{
	// calculate row size and validate columns
	int size = data_size_array() + data_size_array_end();
	list_foreach(&self->list)
	{
		auto key   = list_at(Key, link);
		auto ref   = stack_at(stack, self->list_count - key->order);
		bool error = false;
		switch (key->type) {
		case TYPE_INT:
		{
			if (unlikely(ref->type != VALUE_INT)) {
				error = true;
				break;
			}
			size += data_size_integer(ref->integer);
			break;
		}
		case TYPE_TIMESTAMP:
		{
			if (unlikely(ref->type != VALUE_TIMESTAMP)) {
				error = true;
				break;
			}
			size += data_size_timestamp(ref->integer);
			break;
		}
		case TYPE_STRING:
			if (unlikely(ref->type != VALUE_STRING)) {
				error = true;
				break;
			}
			size += data_size_string(str_size(&ref->string));
			break;
		}
		if (unlikely(error))
			error("column <%.*s>: does not match key data type",
			      str_size(&key->column->name),
			      str_of(&key->column->name));
	}

	row->row = row_allocate(size);

	// copy keys and indexate
	uint8_t* start = row_data(row->row);
	uint8_t* pos = start;
	data_write_array(&pos);
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		auto ref = stack_at(stack, self->list_count - key->order);
		row->key[key->order] = pos - start;
		switch (key->type) {
		case TYPE_INT:
			data_write_integer(&pos, ref->integer);
			break;
		case TYPE_TIMESTAMP:
			data_write_timestamp(&pos, ref->integer);
			break;
		case TYPE_STRING:
			data_write_string(&pos, &ref->string);
			break;
		}
	}
	data_write_array_end(&pos);
}
