
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>

hot Row*
row_create(Keys* keys, uint8_t** pos)
{
	// validate columns and indexate key
	uint32_t index[keys->list_count];

	// []
	uint8_t* data = *pos;	
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	data_read_array(pos);

	list_foreach(&keys->columns->list)
	{
		auto column = list_at(Column, link);
		if (unlikely(data_is_array_end(*pos)))
			error("row has incorrect number of columns");

		// validate column data type
		if (data_is_null(*pos))
		{
			// NOT NULL constraint
			if (unlikely(column->constraint.not_null))
				error("column <%.*s>: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, *pos))) {
			error("column <%.*s>: does not match data type",
			      str_size(&column->name),
			      str_of(&column->name));
		}

		// indexate keys per column
		if (column->key)
		{
			list_foreach(&keys->list)
			{
				auto key = list_at(Key, link);
				if (key->column != column)
					continue;

				// find key path and validate data type
				uint8_t* pos_key = *pos;
				key_find(key, &pos_key);

				// set key index
				index[key->order] = pos_key - data;
			}
		}

		data_skip(pos);
	}

	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");

	// create row
	uint32_t data_size = *pos - data;
	auto self = row_allocate(keys, data_size);
	for (int i = 0; i < keys->list_count; i++)
		row_key_set(self, i, index[i]);
	memcpy(row_data(self, keys), data, data_size);
	return self;
}

hot Row*
row_create_secondary(Keys* keys, Row* row_primary)
{
	// secondary index has shared columns, but different keys
	assert(keys->primary);

	uint32_t index[keys->list_count];
	uint8_t* data = row_data(row_primary, keys->primary);
	uint8_t* pos  = data;
	data_read_array(&pos);

	// indexate primary row data using secondary keys
	list_foreach(&keys->columns->list)
	{
		auto column = list_at(Column, link);
		if (column->key)
		{
			list_foreach(&keys->list)
			{
				auto key = list_at(Key, link);
				if (key->column != column)
					continue;

				// find key path and validate data type
				uint8_t* pos_key = pos;
				key_find(key, &pos_key);

				// set key index
				index[key->order] = pos_key - data;
			}
		}

		data_skip(&pos);
	}

	// create secondary row
	auto self = row_allocate(keys, sizeof(Row*));
	for (int i = 0; i < keys->list_count; i++)
		row_key_set(self, i, index[i]);
	memcpy(row_data(self, keys), &row_primary, sizeof(Row**));
	self->ref = true;
	return self;
}

hot uint32_t
row_hash(Keys* keys, uint8_t** pos)
{
	// validate columns and hash key

	// []
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	data_read_array(pos);

	uint32_t hash = 0;
	list_foreach(&keys->columns->list)
	{
		auto column = list_at(Column, link);
		if (unlikely(data_is_array_end(*pos)))
			error("row has incorrect number of columns");

		// validate column data type
		if (data_is_null(*pos))
		{
			// NOT NULL constraint
			if (unlikely(column->constraint.not_null))
				error("column <%.*s>: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, *pos))) {
			error("column <%.*s>: does not match data type",
			      str_size(&column->name),
			      str_of(&column->name));
		}

		// indexate keys per column
		if (column->key)
		{
			list_foreach(&keys->list)
			{
				auto key = list_at(Key, link);
				if (key->column != column)
					continue;

				// find key path and validate data type
				uint8_t* pos_key = *pos;
				key_find(key, &pos_key);

				// hash key
				hash = key_hash(hash, pos_key);
			}
		}

		data_skip(pos);
	}

	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");

	return hash;
}
