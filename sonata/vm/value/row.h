#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

hot static inline void
value_ref(Keys* self, Ref* row, Stack* stack)
{
	// calculate row size and validate columns
	int size = data_size_array() + data_size_array_end();
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		auto ref = stack_at(stack, self->list_count - key->order);
		if (key->type == TYPE_STRING)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("column <%.*s>: does not match key data type",
				      str_size(&key->column->name),
				      str_of(&key->column->name));
			size += data_size_string(str_size(&ref->string));
		} else
		{
			if (unlikely(ref->type != VALUE_INT))
				error("column <%.*s>: does not match key data type",
				      str_size(&key->column->name),
				      str_of(&key->column->name));
			size += data_size_integer(ref->integer);
		}
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
		if (key->type == TYPE_STRING)
			data_write_string(&pos, &ref->string);
		else
			data_write_integer(&pos, ref->integer);

	}
	data_write_array_end(&pos);
}
