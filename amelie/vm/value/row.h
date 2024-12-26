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

Row* row_create(Columns*, Value*);
Row* row_create_key(Keys*, Value*);
Row* row_update(Row*, Columns*, Value*, int);
void row_update_stored(Columns*, Keys*, Value*, Value*, uint32_t*);

static inline uint32_t
row_value_hash(Keys* keys, Value* row)
{
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto column = list_at(Key, link)->column;
		hash = value_hash(row + column->order, hash);
	}
	return hash;
}
