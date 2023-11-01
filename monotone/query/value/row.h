#pragma once

//
// monotone
//
// SQL OLTP database
//

hot static inline Row*
value_row_key(Def* self, Stack* stack)
{
	// calculate row size and validate columns
	int size = data_size_array(self->key_count);

	auto key = self->key;
	for (; key; key = key->next)
	{
		auto ref = stack_at(stack, self->key_count - key->order);
		auto column = def_column_of(self, key->column);
		if (key->type == TYPE_STRING)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("column <%.*s>: does not match key data type",
				      str_size(&column->name),
				      str_of(&column->name));
			size += data_size_string(str_size(&ref->string));
		} else
		{
			if (unlikely(ref->type != VALUE_INT))
				error("column <%.*s>: does not match key data type",
				      str_size(&column->name),
				      str_of(&column->name));
			size += data_size_integer(ref->integer);
		}
	}

	auto row = row_allocate(self, size);

	// copy keys and indexate
	uint8_t* start = row_data(row, self);
	uint8_t* pos = start;
	data_write_array(&pos, self->key_count);

	key = self->key;
	for (; key; key = key->next)
	{
		auto ref = stack_at(stack, self->key_count - key->order);
		row_key_set_index(row, self, key->order, pos - start);
		if (key->type == TYPE_STRING)
			data_write_string(&pos, &ref->string);
		else
			data_write_integer(&pos, ref->integer);
	}

	return row;
}
