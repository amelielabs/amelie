
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
#include <sonata_def.h>

hot Row*
row_create(Def* def, uint8_t** pos)
{
	// validate columns and indexate key
	uint32_t index[def->key_count];
	uint32_t index_size[def->key_count];

	// []
	uint8_t* data = *pos;	
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	int count;
	data_read_array(pos, &count);
	if (unlikely(count != def->column_count))
		error("row has incorrect number of columns");

	auto column = def->column;
	for (; column; column = column->next)
	{
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
		auto key = column->key;
		for (; key; key = key->next_column)
		{
			// find key path and validate data type
			uint8_t* pos_key = *pos;
			key_find(column, key, &pos_key);

			// set key index
			index[key->order] = pos_key - data;
			uint8_t* pos_key_end = pos_key;
			data_skip(&pos_key_end);
			index_size[key->order] = pos_key_end - pos_key;
		}

		data_skip(pos);
	}

	// create row
	uint32_t data_size = *pos - data;
	auto self = row_allocate(def, data_size);
	for (int i = 0; i < def->key_count; i++)
		row_key_set_index(self, def, i, index[i]);
	memcpy(row_data(self, def), data, data_size);
	return self;
}

hot uint32_t
row_hash(Def* def, uint8_t** pos)
{
	// validate columns and hash key

	// []
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	int count;
	data_read_array(pos, &count);
	if (unlikely(count != def->column_count))
		error("row has incorrect number of columns");

	uint32_t hash = 0;
	for (auto column = def->column; column; column = column->next)
	{
		// validate column data type
		if (data_is_null(*pos))
		{
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

		// hash keys per column
		auto key = column->key;
		for (; key; key = key->next_column)
		{
			// find key path and validate data type
			uint8_t* pos_key = *pos;

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

		data_skip(pos);
	}

	return hash;
}
