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

hot static inline int
compare(Keys* self, Row* a, Row* b)
{
	list_foreach(&self->list)
	{
		const auto column = list_at(Key, link)->column;
		int64_t rc;
		if (column->type_size == 4)
		{
			// int
			rc = compare_int32(*(int32_t*)row_at(a, column->order),
			                   *(int32_t*)row_at(b, column->order));
		} else
		if (column->type_size == 8)
		{
			// int64, timestamp
			rc = compare_int64(*(int64_t*)row_at(a, column->order),
			                   *(int64_t*)row_at(b, column->order));
		} else
		if (column->type_size == sizeof(Uuid))
		{
			// uuid
			rc = uuid_compare(row_at(a, column->order),
			                  row_at(b, column->order));
		} else
		{
			// string
			rc = json_compare_string(row_at(a, column->order),
			                         row_at(b, column->order));
		}
		if (rc != 0)
			return rc;
	}
	return 0;
}
