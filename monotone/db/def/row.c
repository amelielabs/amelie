
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
#include <monotone_def.h>

hot Row*
row_create(Def* def, uint8_t* data, int data_size)
{
	// validate columns and indexate key
	uint32_t index[def->key_count];
	uint32_t index_size[def->key_count];

	// []
	uint8_t* pos = data;
	if (unlikely(! data_is_array(pos)))
		error("row type expected to be array");
	int count;
	data_read_array(&pos, &count);
	if (unlikely(count != def->column_count))
		error("row has insufficient number of columns");

	bool is_partial = false;
	auto column = def->column;
	for (; column; column = column->next)
	{
		// validate column data type
		if (data_is_partial(pos))
		{
			is_partial = true;
			assert(! column->key);
		} else
		{
			if (unlikely(! type_validate(column->type, pos)))
				error("column <%.*s>: does not match data type",
				      str_size(&column->name),
				      str_of(&column->name));
		}

		// indexate keys per column
		auto key = column->key;
		for (; key; key = key->next_column)
		{
			// find key path and validate data type
			uint8_t* pos_key = pos;

			// find by path
			if (! str_empty(&key->path))
			{
				if (! map_find_path(&pos_key, &key->path))
					error("column %.*s: key path <%.*s> is not found",
					      str_size(&column->name),
					      str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));

				// validate data type
				if (! type_validate(key->type, pos_key))
					error("column %.*s: key path <%.*s> does not match data type",
					      str_size(&column->name),
					      str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));
			}

			// set key index
			index[key->order] = pos_key - data;
			uint8_t* pos_key_end = pos_key;
			data_skip(&pos_key_end);
			index_size[key->order] = pos_key_end - pos_key;
		}

		data_skip(&pos);
	}

	// create row
	auto self = row_allocate(def, data_size);
	self->is_partial = is_partial;
	for (int i = 0; i < def->key_count; i++)
		row_key_set_index(self, def, i, index[i]);
	memcpy(row_data(self, def), data, data_size);
	return self;
}

hot uint32_t
row_hash(Def* def, uint8_t* data, int data_size)
{
	// validate columns and hash key
	unused(data_size);

	// []
	uint8_t* pos = data;
	if (unlikely(! data_is_array(pos)))
		error("row type expected to be array");
	int count;
	data_read_array(&pos, &count);
	if (unlikely(count != def->column_count))
		error("row has insufficient number of columns");

	uint32_t hash = 0;
	for (auto column = def->column; column; column = column->next)
	{
		// validate column data type
		if (likely(! data_is_partial(pos)))
		{
			if (unlikely(! type_validate(column->type, pos)))
				error("column <%.*s>: does not match data type",
				      str_size(&column->name),
				      str_of(&column->name));
		}

		// hash keys per column
		auto key = column->key;
		for (; key; key = key->next_column)
		{
			// find key path and validate data type
			uint8_t* pos_key = pos;

			// find by path
			if (! str_empty(&key->path))
			{
				if (! map_find_path(&pos_key, &key->path))
					error("column %.*s: key path <%.*s> is not found",
					      str_size(&column->name),
					      str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));

				// validate data type
				if (! type_validate(key->type, pos_key))
					error("column %.*s: key path <%.*s> does not match data type",
					      str_size(&column->name),
					      str_of(&column->name),
					      str_size(&key->path),
					      str_of(&key->path));
			}

			// hash key
			uint8_t* pos_key_end = pos_key;
			data_skip(&pos_key_end);
			hash = hash_murmur3_32(pos_key, pos_key_end - pos_key, hash);
		}

		data_skip(&pos);
	}

	return hash;
}
