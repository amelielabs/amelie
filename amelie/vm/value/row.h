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

Row* row_create(Heap*, Columns*, Value*, Value*, int64_t);
Row* row_create_key(Buf*, Keys*, Value*, int);
Row* row_update(Heap*, Row*, Columns*, Value*, int);

static inline uint32_t
row_value_hash(Keys* keys, Value* refs, Value* row, int64_t identity)
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

static inline int64_t
row_get_identity(Table* table, Value* refs, Value* row)
{
	auto columns = table_columns(table);
	if (! columns->identity)
		return 0;

	// get identity column value
	auto ref = row + columns->identity->order;
	if (ref->type == TYPE_REF)
		ref = &refs[ref->integer];
	if (ref->type != TYPE_NULL)
		return ref->integer;

	// generate identity column value
	auto cons = &columns->identity->constraints;
	if (cons->as_identity == IDENTITY_SERIAL)
		return sequence_next(&table->seq);

	return random_generate(&runtime()->random) % cons->as_identity_modulo;
}
