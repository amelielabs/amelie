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

Row* row_create(Heap*, Columns*, Value*, Value*);
Row* row_create_key(Buf*, Keys*, Value*, int);
Row* row_update(Heap*, Row*, Columns*, Value*, int);

static inline uint32_t
row_value_hash(Keys* keys, Value* refs, Value* row)
{
	uint32_t hash = 0;
	list_foreach(&keys->list)
	{
		auto column = list_at(Key, link)->column;
		auto value = row + column->order;
		if (value->type == TYPE_REF)
		{
			// this condition checked by PUSH_REF
			value = &refs[value->integer];
			assert(value->type != TYPE_NULL);
		}
		hash = value_hash(value, column->type_size, hash);
	}
	return hash;
}
