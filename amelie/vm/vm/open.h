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

typedef struct Open Open;

struct Open
{
	Table*       table;
	IndexConfig* index;
	Branch*      branch;
	int          keys_count;
	bool         point_lookup;
	bool         open_part;
};

static inline Open*
open_create(CodeData* data, int* offset)
{
	*offset = code_data_pos(data);
	auto open = (Open*)buf_emplace(&data->data, sizeof(Open));
	return open;
}

static inline Open*
open_at(CodeData* data, int offset)
{
	return (Open*)code_data_at(data, offset);
}
