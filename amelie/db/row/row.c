
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

hot Row*
row_create(Columns* columns, uint8_t** pos)
{
	// []
	uint8_t* data = *pos;	
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	data_read_array(pos);

	// validate columns
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (unlikely(data_is_array_end(*pos)))
			error("row has incorrect number of columns");

		// validate column data type
		if (data_is_null(*pos))
		{
			// NOT NULL constraint
			if (unlikely(column->constraint.not_null))
				error("column %.*s: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, *pos))) {
			error("column %.*s: does not match data type %s",
			      str_size(&column->name),
			      str_of(&column->name),
			      type_of(column->type));
		}

		data_skip(pos);
	}

	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");

	// create row
	uint32_t data_size = *pos - data;
	auto self = row_allocate(data_size);
	memcpy(row_data(self), data, data_size);
	return self;
}

hot void
row_create_hash(Keys* keys, uint32_t* hash, uint8_t** pos)
{
	// validate columns and hash key

	// []
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
				error("column %.*s: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, *pos))) {
			error("column %.*s: does not match data type %s",
			      str_size(&column->name),
			      str_of(&column->name),
			      type_of(column->type));
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
				*hash = key_hash(*hash, pos_key);
			}
		}

		data_skip(pos);
	}

	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");
}

Row*
row_alter_add(Row* row, Buf* append)
{
	auto self = row_allocate(row_size(row) + buf_size(append));
	int  offset = row_size(row) - data_size_array_end();
	memcpy(row_data(self), row_data(row), offset);
	memcpy(row_data(self) + offset, append->start, buf_size(append));
	offset += buf_size(append);
	uint8_t* pos = row_data(self) + offset;
	data_write_array_end(&pos);
	return self;
}

Row*
row_alter_drop(Row* row, int order)
{
	uint8_t* start = row_data(row);
	uint8_t* end   = row_data(row) + row_size(row);

	// before column and after column
	uint8_t* pos        = start;
	array_find(&pos, order);
	uint8_t* pos_before = pos;
	data_skip(&pos);
	uint8_t* pos_after  = pos;

	int size = (pos_before - start) + (end - pos_after);
	auto self = row_allocate(size);
	pos = row_data(self);
	memcpy(pos, start, pos_before - start);
	pos += pos_before - start;
	memcpy(pos, pos_after, end - pos_after);
	return self;
}
