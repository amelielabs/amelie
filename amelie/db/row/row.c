
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
row_alter_add(Row* row, Buf* append)
{
	(void)row;
	(void)append;
	// TODO
	return NULL;
#if 0
	auto self = row_allocate(row_size(row) + buf_size(append));
	int  offset = row_size(row) - data_size_array_end();
	memcpy(row_data(self), row_data(row), offset);
	memcpy(row_data(self) + offset, append->start, buf_size(append));
	offset += buf_size(append);
	uint8_t* pos = row_data(self) + offset;
	data_write_array_end(&pos);
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
	data_skip(&pos);
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
