
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
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>

hot static inline int
row_create_prepare(Columns* columns, Value* values)
{
	auto size = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto value = values + column->order;

		// null
		if (value->type == TYPE_NULL)
		{
			if (column->constraints.not_null)
			{
				// NOT NULL constraint
				if (unlikely(column->constraints.not_null))
					error("column <%.*s> cannot be NULL", str_size(&column->name),
					      str_of(&column->name));
			}
			continue;
		}

		// fixed types
		if (column->type_size > 0)
		{
			size += column->type_size;
			continue;
		}

		// variable types
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
			abort();
			break;
		}
	}
	return size;
}

hot Row*
row_create(Heap* heap, Columns* columns, Value* values)
{
	auto     row_size = row_create_prepare(columns, values);
	auto     row = row_allocate(heap, columns->count, row_size);
	uint8_t* pos = row_data(row, columns->count);
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
		case TYPE_DATE:
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
		case TYPE_UUID:
			*(Uuid*)pos = value->uuid;
			pos += sizeof(Uuid);
			break;
		}
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

		if (value)
		{
			// null
			if (value->type == TYPE_NULL)
			{
				if (column->constraints.not_null)
				{
					// NOT NULL constraint
					if (unlikely(column->constraints.not_null))
						error("column <%.*s> cannot be NULL", str_size(&column->name),
						      str_of(&column->name));
				}
				continue;
			}

			// fixed types
			if (column->type_size > 0)
			{
				size += column->type_size;
				continue;
			}

			// variable types
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
				abort();
				break;
			}
			continue;
		}

		// null
		uint8_t* pos_src = row_at(self, column->order);
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
			case TYPE_DATE:
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
			case TYPE_UUID:
				*(Uuid*)pos = value->uuid;
				pos += sizeof(Uuid);
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
