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

hot static inline Row*
value_row_key(Keys* self, Stack* stack)
{
	int size = 0;
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		switch (key->column->type) {
		case TYPE_INT8:
		case TYPE_INT16:
		case TYPE_INT32:
		case TYPE_INT64:
		case TYPE_TIMESTAMP:
			size += key->column->type_size;
			break;
		case TYPE_TEXT:
		{
			auto ref = stack_at(stack, self->list_count - key->order);
			size += data_size_string(str_size(&ref->string));
			break;
		}
		}
	}

	auto     row   = row_allocate(self->columns->list_count, size);
	uint8_t* pos   = row_data(row);
	auto     order = 0;
	list_foreach(&self->columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->key)
		{
			row_set_null(row, column->order);
			continue;
		}
		auto ref = stack_at(stack, self->list_count - order);
		order++;

		// set index to pos
		row_set_ptr(row, column->order, pos);
		switch (column->type) {
		case TYPE_INT8:
			*(int8_t*)pos = ref->integer;
			pos += sizeof(int8_t);
			break;
		case TYPE_INT16:
			*(int16_t*)pos = ref->integer;
			pos += sizeof(int16_t);
			break;
		case TYPE_INT32:
			*(int32_t*)pos = ref->integer;
			pos += sizeof(int32_t);
			break;
		case TYPE_INT64:
			*(int64_t*)pos = ref->integer;
			pos += sizeof(int64_t);
			break;
		case TYPE_TIMESTAMP:
			*(uint64_t*)pos = ref->integer;
			pos += sizeof(int64_t);
			break;
		case TYPE_TEXT:
			data_write_string(&pos, &ref->string);
			break;
		}
	}

	return row;
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
