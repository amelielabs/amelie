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

static inline uint32_t
target_lookup_hash(Target* target)
{
	uint32_t hash = 0;
	list_foreach(&target->from_table_index->keys.list)
	{
		auto key    = list_at(Key, link);
		auto column = key->column;
		auto ref    = &target->plan->keys[key->order];
		assert(ref->start);
		if (column->type == TYPE_STRING)
		{
			hash = hash_murmur3_32((uint8_t*)str_u8(&ref->start->string),
			                       str_size(&ref->start->string),
			                       0);
		} else
		{
			// timestamp or int
			hash = hash_murmur3_32((uint8_t*)&ref->start->integer,
			                       sizeof(ref->start->integer),
			                       0);
		}
	}
	return hash;
}
