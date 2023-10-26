
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
#include <monotone_key.h>

hot Row*
row_create(Key* key, uint8_t* data, int data_size)
{
	// validate columns and indexate key
	uint32_t index[key->key_count];
	uint32_t index_size[key->key_count];

	uint8_t* pos = data;
	if (unlikely(! data_is_array(pos)))
		error("row type expected to be array");
	int count;
	data_read_array(&pos, &count);

	bool is_partial = false;
	auto column = key->column;
	for (; column; column = column->next)
	{
		// validate data type
		if (data_is_partial(pos))
		{
			is_partial = true;
			if (unlikely(column_is_key(column)))
				error("column <%.*s>: partial cannot be a key",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		{
			if (unlikely(! type_validate(column->type, pos)))
				error("column <%.*s>: does not match data type",
				      str_size(&column->name),
				      str_of(&column->name));
		}
		if (! column_is_key(column))
		{
			data_skip(&pos);
			continue;
		}

		// set key index
		index[column->order_key] = pos - data;
		uint8_t* pos_start = pos;
		data_skip(&pos);
		index_size[column->order_key] = pos - pos_start;
	}

	// create row
	auto self = row_allocate(key, data_size);
	self->is_partial = is_partial;
	for (int i = 0; i < key->key_count; i++)
		row_key_set_index(self, key, i, index[i]);
	memcpy(row_data(self, key), data, data_size);
	return self;
}
