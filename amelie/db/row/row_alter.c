
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>

Row*
row_alter_add(Row* row, Buf* value)
{
	(void)row;
	(void)value;
	// TODO
	return NULL;
#if 0
	auto self = row_allocate(row_size(row) + buf_size(append));
	int  offset = row_size(row) - json_size_array_end();
	memcpy(row_data(self), row_data(row), offset);
	memcpy(row_data(self) + offset, append->start, buf_size(append));
	offset += buf_size(append);
	uint8_t* pos = row_data(self) + offset;
	json_write_array_end(&pos);
	return self;
#endif
}

Row*
row_alter_drop(Row* row, int order)
{
	(void)row;
	(void)order;
	// TODO
	return NULL;
#if 0
	uint8_t* start = row_data(row);
	uint8_t* end   = row_data(row) + row_size(row);

	// before column and after column
	uint8_t* pos        = start;
	array_find(&pos, order);
	uint8_t* pos_before = pos;
	json_skip(&pos);
	uint8_t* pos_after  = pos;

	int size = (pos_before - start) + (end - pos_after);
	auto self = row_allocate(size);
	pos = row_data(self);
	memcpy(pos, start, pos_before - start);
	pos += pos_before - start;
	memcpy(pos, pos_after, end - pos_after);
	return self;
#endif
}

#if 0
hot static inline uint32_t
value_row_generate(Columns*  columns,
                   Keys*     keys,
                   Buf*      data,
                   uint8_t** pos,
                   Value*    values)
{
	// [
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	data_read_array(pos);
	encode_array(data);

	uint32_t hash = 0;
	int order = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (unlikely(data_is_array_end(*pos)))
			error("row has incorrect number of columns");

		// skip column data
		auto at = *pos;
		data_skip(pos);

		// use generated or existing column
		int offset = buf_size(data);
		if (! str_empty(&column->constraint.as_stored))
		{
			value_write(&values[order], data);
			order++;
		} else
		{
			// copy
			buf_write(data, at, *pos - at);
		}

		// validate column data type
		auto pos_new = data->start + offset;
		if (data_is_null(pos_new))
		{
			// NOT NULL constraint
			if (unlikely(column->constraint.not_null))
				error("column %.*s: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, pos_new))) {
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
				uint8_t* pos_key = pos_new;
				key_find(key, &pos_key);

				// hash key
				hash = key_hash(hash, pos_key);
			}
		}
	}

	// ]
	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");
	encode_array_end(data);
	return hash;
}
#endif
