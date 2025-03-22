
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
#include <amelie_heap.h>

Row*
row_alter_add(Heap* heap, Row* row, Columns* columns)
{
	// allocate new row (row index size might change)
	auto columns_count = columns->count + 1;
	auto self = row_allocate(heap, columns_count, row_data_size(row, columns->count));

	// copy original columns
	uint8_t* pos = row_data(self, columns_count);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		// null
		uint8_t* data = row_at(row, column->order);
		if (! data)
		{
			row_set_null(self, column->order);
			continue;
		}
		row_set(self, column->order, pos - (uint8_t*)self);
		switch (column->type) {
		case TYPE_STRING:
		case TYPE_JSON:
		{
			auto pos_end = data;
			json_skip(&pos_end);
			memcpy(pos, data, pos_end - data);
			pos += pos_end - data;
			break;
		}
		case TYPE_VECTOR:
			memcpy(pos, data, vector_size((Vector*)data));
			pos += vector_size((Vector*)data);
			break;
		default:
			memcpy(pos, data, column->type_size);
			pos += column->type_size;
			break;
		}
	}

	// set new column as null
	row_set_null(self, columns->count);
	return self;
}

Row*
row_alter_drop(Heap* heap, Row* row, Columns* columns, Column* ref)
{
	// calculate row data size without the column data
	auto size = row_data_size(row, columns->count);
	uint8_t* data = row_at(row, ref->order);
	if (data)
	{
		switch (ref->type) {
		case TYPE_STRING:
		case TYPE_JSON:
		{
			auto pos_end = data;
			json_skip(&pos_end);
			size -= pos_end - data;
			break;
		}
		case TYPE_VECTOR:
			size -= vector_size((Vector*)data);
			break;
		default:
			size -= ref->type_size;
			break;
		}
	}

	// allocate new row
	auto self = row_allocate(heap, columns->count - 1, size);

	// copy original columns
	uint8_t* pos = row_data(self, columns->count - 1);
	auto order = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (column == ref)
			continue;

		// null
		uint8_t* data = row_at(row, column->order);
		if (! data)
		{
			row_set_null(self, order);
			order++;
			continue;
		}
		row_set(self, order, pos - (uint8_t*)self);
		order++;
		switch (column->type) {
		case TYPE_STRING:
		case TYPE_JSON:
		{
			auto pos_end = data;
			json_skip(&pos_end);
			memcpy(pos, data, pos_end - data);
			pos += pos_end - data;
			break;
		}
		case TYPE_VECTOR:
			memcpy(pos, data, vector_size((Vector*)data));
			pos += vector_size((Vector*)data);
			break;
		default:
			memcpy(pos, data, column->type_size);
			pos += column->type_size;
			break;
		}
	}

	return self;
}
