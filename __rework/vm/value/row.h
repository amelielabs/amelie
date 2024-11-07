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
