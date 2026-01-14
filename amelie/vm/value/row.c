
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>

hot static inline int
row_create_prepare(Columns* columns, Value* values, Value* refs)
{
	auto size = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		auto value = values + column->order;

		// use reference
		if (value->type == TYPE_REF)
			value = &refs[value->integer];

		// null
		if (value->type == TYPE_NULL)
		{
			// identity column
			if (column->constraints.as_identity)
			{
				size += column->type_size;
				continue;
			}

			// NOT NULL constraint
			if (unlikely(column->constraints.not_null))
				error("column '%.*s' cannot be NULL", str_size(&column->name),
				      str_of(&column->name));
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
row_create(Heap*   heap, Columns* columns, Value* values, Value* refs,
           int64_t identity)
{
	auto     row_size = row_create_prepare(columns, values, refs);
	auto     row = row_allocate(heap, columns->count, row_size);
	uint8_t* pos = row_data(row, columns->count);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// use reference
		auto value = &values[column->order];
		if (value->type == TYPE_REF)
			value = &refs[value->integer];

		// null
		if (value->type == TYPE_NULL)
		{
			// set identity column value
			if (columns->identity == column)
			{
				row_set(row, column->order, pos - (uint8_t*)row);
				*(int64_t*)pos = identity;
				pos += sizeof(int64_t);
				continue;
			}

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
						error("column '%.*s' cannot be NULL", str_size(&column->name),
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
		uint8_t* pos_src = row_column(self, column->order);
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
		uint8_t* pos_src = row_column(self, column->order);
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

static inline uint32_t
row_map_hash(Keys* keys, Value* refs, Value* row, int64_t identity)
{
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto column = list_at(Key, link)->column;
		auto value = row + column->order;
		if (value->type == TYPE_REF)
			value = &refs[value->integer];
		if (column->constraints.as_identity && value->type == TYPE_NULL)
		{
			hash = hash_murmur3_32((uint8_t*)&identity, sizeof(identity), hash);
			continue;
		}
		assert(value->type != TYPE_NULL);
		hash = value_hash(value, column->type_size, hash);
	}
	return hash;
}

Part*
row_map(Table* table, Value* refs, Value* row, int64_t identity)
{
	auto mapping = &table->volume_mgr.mapping;
	assert(mapping->type == MAPPING_HASH);
	auto hash_partition = row_map_hash(mapping->keys, refs, row, identity) % UINT16_MAX;
	// todo: MAPPING_HASH_RANGE
	return mapping->map[hash_partition];
}

Part*
row_map_key(Table* table, Value* keys)
{
	// compute hash using key values
	uint32_t hash = 0;

	auto index = table_primary(table);
	list_foreach(&index->keys.list)
	{
		auto key = list_at(Key, link);
		hash = value_hash(&keys[key->order], key->column->type_size, hash);
	}

	auto mapping = &table->volume_mgr.mapping;
	assert(mapping->type == MAPPING_HASH);
	// todo: MAPPING_HASH_RANGE
	auto hash_partition = hash % UINT16_MAX;
	return mapping->map[hash_partition];
}
