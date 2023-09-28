#pragma once

//
// monotone
//
// SQL OLTP database
//

hot static inline Row*
value_row_key(Schema* schema, Stack* stack)
{
	// calculate row size and validate columns
	int size = data_size_array(schema->key_count);

	auto key = schema->key;
	for (; key; key = key->next_key)
	{
		auto ref = stack_at(stack, schema->key_count - key->order_key);
		if (key->type == TYPE_STRING)
		{
			if (unlikely(ref->type != VALUE_STRING))
				error("column <%.*s>: does not match data type", 
				      str_size(&key->name),
				      str_of(&key->name));
			size += data_size_string(ref->data_size);
		} else
		{
			if (unlikely(ref->type != VALUE_INT))
				error("column <%.*s>: does not match data type", 
				      str_size(&key->name),
				      str_of(&key->name));
			size += data_size_integer(ref->integer);
		}
	}

	auto row = row_allocate(schema, size);

	// copy keys and indexate
	uint8_t* start = row_data(row, schema);
	uint8_t* pos = start;
	data_write_array(&pos, schema->key_count);

	key = schema->key;
	for (; key; key = key->next_key)
	{
		auto ref = stack_at(stack, schema->key_count - key->order_key);
		row_key_set_index(row, schema, key->order_key, pos - start);
		if (key->type == TYPE_STRING)
			data_write_string(&pos, &ref->string);
		else
			data_write_integer(&pos, ref->integer);
	}

	return row;
}
