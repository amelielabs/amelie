
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>

hot Row*
row_copykey(Heap* heap, Row* self, Columns* columns)
{
	// create a row which has only key columns (others are set to NULL)
	int size = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->refs)
			continue;

		// int, timestamp, uuid
		if (column->size > 0)
		{
			size += column->size;
		} else
		{
			// string
			uint8_t* start = row_column(self, column);
			uint8_t* pos = start;
			data_skip(&pos);
			size += pos - start;
		}
	}

	auto row = row_allocate(heap, self->tsn, self->snapshot, columns->count, size);
	uint8_t* pos = row_data(row, columns->count);
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// column is not a key
		if (! column->refs)
		{
			row_set_null(row, column->order);
			continue;
		}
		row_set(row, column->order, pos - (uint8_t*)row);

		// int, timestamp, uuid
		if (column->size > 0)
		{
			memcpy(row_column(row, column), row_column(self, column), column->size);
		} else
		{
			// string
			uint8_t* start = row_column(self, column);
			uint8_t* pos = start;
			data_skip(&pos);
			memcpy(row_column(row, column), start, pos - start);
		}
	}

	return row;
}
