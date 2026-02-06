
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>

hot Row*
row_create(Heap*  heap, Columns* columns,
           Value* values,
           Value* refs,
           Value* identity)
{
	// calculate row size
	auto size = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto value  = values + column->order;
		size += value_data_size(value, column, refs);
	}

	// create and write row
	auto     row = row_allocate(heap, columns->count, size);
	uint8_t* pos = row_data(row, columns->count);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto offset = pos - (uint8_t*)row;
		if (value_data_write(&values[column->order], column, refs, identity, &pos))
			row_set(row, column->order, offset);
		else
			row_set_null(row, column->order);
	}
	return row;
}

hot Row*
row_create_key(Buf* buf, Keys* self, Value* values, int count)
{
	int size = 0;
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);

		// int, timestamp, uuid
		auto column = key->column;
		if (column->type_size > 0)
		{
			size += key->column->type_size;
			continue;
		}

		// string
		if (key->order < count)
		{
			auto ref = &values[key->order];
			size += json_size_string(str_size(&ref->string));
		} else
		{
			// min string
			size += json_size_string(0);
		}
	}

	auto columns_count = self->columns->count;
	auto row = row_allocate_buf(buf, columns_count, size);
	uint8_t* pos = row_data(row, columns_count);
	list_foreach(&self->columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->refs)
		{
			row_set_null(row, column->order);
			continue;
		}
		auto key = keys_find_column(self, column->order);
		if (! key)
		{
			row_set_null(row, column->order);
			continue;
		}
		row_set(row, column->order, pos - (uint8_t*)row);

		// string
		if (column->type == TYPE_STRING)
		{
			if (key->order < count)
			{
				auto ref = &values[key->order];
				json_write_string(&pos, &ref->string);
			} else
			{
				Str str;
				str_init(&str);
				json_write_string(&pos, &str);
			}
			continue;
		}

		// int, timestamp, uuid
		switch (column->type_size) {
		case 4:
		{
			if (key->order < count)
			{
				auto ref = &values[key->order];
				*(int32_t*)pos = ref->integer;
			} else {
				*(int32_t*)pos = INT32_MIN;
			}
			pos += sizeof(int32_t);
			break;
		}
		case 8:
		{
			if (key->order < count)
			{
				auto ref = &values[key->order];
				*(int64_t*)pos = ref->integer;
			} else
			{
				if (column->type == TYPE_TIMESTAMP)
					*(int64_t*)pos = 0;
				else
					*(int64_t*)pos = INT64_MIN;
			}
			pos += sizeof(int64_t);
			break;
		}
		case sizeof(Uuid):
		{
			if (key->order < count)
			{
				auto ref = &values[key->order];
				*(Uuid*)pos = ref->uuid;
			} else {
				uuid_init((Uuid*)pos);
			}
			pos += sizeof(Uuid);
			break;
		}
		default:
			abort();
			break;
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

		// use value
		if (value)
		{
			size += value_data_size(value, column, NULL);
			continue;
		}

		// null
		uint8_t* pos_src = row_column(self, column);
		if (! pos_src)
			continue;

		// fixed types
		if (column->type_size > 0)
		{
			size += column->type_size;
			continue;
		}

		// variable types
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
			abort();
			break;
		}
	}
	return size;
}

hot Row*
row_update(Heap* heap, Row* self, Columns* columns, Value* values, int count)
{
	// merge source row columns data with updated values
	//
	// [order, value, order, value, ...]
	//
	auto     row_size = row_update_prepare(self, columns, values, count);
	auto     row      = row_allocate(heap, columns->count, row_size);
	uint8_t* pos      = row_data(row, columns->count);

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

		// use value
		auto offset = pos - (uint8_t*)row;
		if (value)
		{
			if (value_data_write(value, column, NULL, NULL, &pos))
				row_set(row, column->order, offset);
			else
				row_set_null(row, column->order);
			continue;
		}

		// null
		uint8_t* pos_src = row_column(self, column);
		if (! pos_src)
		{
			row_set_null(row, column->order);
			continue;
		}

		row_set(row, column->order, offset);
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
