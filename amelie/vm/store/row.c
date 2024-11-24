
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

hot Row*
row_create(Columns* columns, Value* values, int size)
{
	auto     row = row_allocate(columns->list_count, size);
	uint8_t* pos = row_data(row, columns->list_count);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// null
		auto value = &values[column->order];
		if (value->type == TYPE_NULL)
		{
			row_set_null(row, column->order);
			continue;
		}

		row_set(row, column->order, pos - (uint8_t*)row);
		switch (column->type) {
		case TYPE_BOOL:
		case TYPE_INT:
		case TYPE_TIMESTAMP:
		{
			switch (column->type_size) {
			case 1:
				*(int8_t*)pos = value->integer;
				pos += sizeof(int8_t);
				break;
			case 2:
				*(int16_t*)pos = value->integer;
				pos += sizeof(int16_t);
				break;
			case 4:
				*(int32_t*)pos = value->integer;
				pos += sizeof(int32_t);
				break;
			case 8:
				*(int64_t*)pos = value->integer;
				pos += sizeof(int64_t);
				break;
			default:
				abort();
				break;
			}
			break;
		}
		case TYPE_DOUBLE:
		{
			switch (column->type_size) {
			case 4:
				*(float*)pos = value->dbl;
				pos += sizeof(float);
				break;
			case 8:
				*(double*)pos = value->dbl;
				pos += sizeof(double);
				break;
			default:
				abort();
				break;
			}
			break;
		}
		case TYPE_INTERVAL:
			*(Interval*)pos = value->interval;
			pos += sizeof(Interval);
			break;
		case TYPE_STRING:
			json_write_string(&pos, &value->string);
			break;
		case TYPE_JSON:
			memcpy(pos, value->json, value->json_size);
			pos += value->json_size;
			break;
		case TYPE_VECTOR:
			memcpy(pos, value->vector, vector_size(value->vector));
			pos += vector_size(value->vector);
			break;
		}
	}

	return row;
}

Row*
row_create_key(Keys* self, Value* values)
{
	int size = 0;
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		if (key->column->type == TYPE_STRING) {
			auto ref = &values[key->order];
			size += json_size_string(str_size(&ref->string));
		} else
		{
			// int, timestamp
			size += key->column->type_size;
		}
	}

	auto     columns_count = self->columns->list_count;
	auto     row   = row_allocate(columns_count, size);
	uint8_t* pos   = row_data(row, columns_count);
	auto     order = 0;
	list_foreach(&self->columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->key)
		{
			row_set_null(row, column->order);
			continue;
		}
		auto ref = &values[order];
		order++;

		row_set(row, column->order, pos - (uint8_t*)row);
		if (column->type == TYPE_STRING) {
			json_write_string(&pos, &ref->string);
		} else
		{
			// int, timestamp
			switch (column->type_size) {
			case 1:
				*(int8_t*)pos = ref->integer;
				pos += sizeof(int8_t);
				break;
			case 2:
				*(int16_t*)pos = ref->integer;
				pos += sizeof(int16_t);
				break;
			case 4:
				*(int32_t*)pos = ref->integer;
				pos += sizeof(int32_t);
				break;
			case 8:
				*(int64_t*)pos = ref->integer;
				pos += sizeof(int64_t);
				break;
			default:
				abort();
				break;
			}
		}
	}

	return row;
}

hot static inline int
row_update_prepare(Row* self, Columns* columns, Value* values, int count)
{
	auto order = 0;
	auto size  = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		Value* value = NULL;
		if (order < count && values[order * 2].integer == column->order)
		{
			value = &values[order * 2 + 1];
			order++;
		}

		if (value)
		{
			// null
			if (value->type == TYPE_NULL)
			{
				if (column->constraint.not_null)
				{
					// NOT NULL constraint
					if (unlikely(column->constraint.not_null))
						error("column %.*s: cannot be null", str_size(&column->name),
						      str_of(&column->name));
				}
				continue;
			}

			switch (column->type) {
			case TYPE_STRING:
				size += json_size_string(str_size(&value->string));
				break;
			case TYPE_JSON:
				size += value->json_size;
				break;
			case TYPE_VECTOR:
				size += vector_size(value->vector);
				break;
			default:
				// fixed types
				size += column->type_size;
				break;
			}
			continue;
		}

		// null
		uint8_t* pos_src = row_at(self, column->order);
		if (! pos_src)
			continue;

		switch (column->type) {
		case TYPE_STRING:
		case TYPE_JSON:
		{
			auto pos_end = pos_src;
			json_skip(&pos_end);
			size += pos_end - pos_src;
			break;
		}
		case TYPE_VECTOR:
			size += vector_size((Vector*)pos_src);
			break;
		default:
			// fixed types
			size += column->type_size;
			break;
		}
	}
	return size;
}

hot Row*
row_update(Row* self, Columns* columns, Value* values, int count)
{
	// merge source row columns data with columns on stack
	//
	// [order, value, order, value, ...]
	//
	auto     row_size = row_update_prepare(self, columns, values, count);
	auto     row      = row_allocate(columns->list_count, row_size);
	uint8_t* pos      = row_data(row, columns->list_count);

	auto order = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		Value* value = NULL;
		if (order < count && values[order * 2].integer == column->order)
		{
			value = &values[order * 2 + 1];
			order++;
		}

		if (value)
		{
			// null
			if (value->type == TYPE_NULL)
			{
				row_set_null(row, column->order);
				continue;
			}

			row_set(row, column->order, pos - (uint8_t*)row);
			switch (column->type) {
			case TYPE_BOOL:
			case TYPE_INT:
			case TYPE_TIMESTAMP:
			{
				switch (column->type_size) {
				case 1:
					*(int8_t*)pos = value->integer;
					pos += sizeof(int8_t);
					break;
				case 2:
					*(int16_t*)pos = value->integer;
					pos += sizeof(int16_t);
					break;
				case 4:
					*(int32_t*)pos = value->integer;
					pos += sizeof(int32_t);
					break;
				case 8:
					*(int64_t*)pos = value->integer;
					pos += sizeof(int64_t);
					break;
				default:
					abort();
					break;
				}
				break;
			}
			case TYPE_DOUBLE:
			{
				switch (column->type_size) {
				case 4:
					*(float*)pos = value->dbl;
					pos += sizeof(float);
					break;
				case 8:
					*(double*)pos = value->dbl;
					pos += sizeof(double);
					break;
				default:
					abort();
					break;
				}
				break;
			}
			case TYPE_INTERVAL:
				*(Interval*)pos = value->interval;
				pos += sizeof(Interval);
				break;
			case TYPE_STRING:
				json_write_string(&pos, &value->string);
				break;
			case TYPE_JSON:
				memcpy(pos, value->json, value->json_size);
				pos += value->json_size;
				break;
			case TYPE_VECTOR:
				memcpy(pos, value->vector, vector_size(value->vector));
				pos += vector_size(value->vector);
				break;
			}

			continue;
		}

		// null
		uint8_t* pos_src = row_at(self, column->order);
		if (! pos_src)
		{
			row_set_null(row, column->order);
			continue;
		}

		row_set(row, column->order, pos - (uint8_t*)row);
		switch (column->type) {
		case TYPE_STRING:
		case TYPE_JSON:
		{
			auto pos_end = pos_src;
			json_skip(&pos_end);
			memcpy(pos, pos_src, pos_end - pos_src);
			pos += pos_end - pos_src;
			break;
		}
		case TYPE_VECTOR:
			memcpy(pos, pos_src, vector_size((Vector*)pos_src));
			pos += vector_size((Vector*)pos_src);
			break;
		default:
			// fixed column types
			memcpy(pos, pos_src, column->type_size);
			pos += column->type_size;
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
