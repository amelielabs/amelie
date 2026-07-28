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

Row* row_create_key(Buf*, Keys*, Value*, int);
Row* row_create(Part*, Timeline*, Columns*, Value*, Value*, Value*);
Row* row_update(Part*, Timeline*, Columns*, Row*, Value*, int);

hot static inline void
row_identity(Column* column, Value* refs,
             Value*  row,
             Value*  identity,
             Local*  local)
{
	// get existing identity value
	auto value = row + column->order;
	if (value->type == TYPE_REF)
		value = &refs[value->integer];
	if (value->type != TYPE_NULL)
	{
		*identity = *value;
		return;
	}

	// generate
	auto cons = &column->constraints;
	if (cons->as_identity == IDENTITY_RANDOM)
	{
		uint64_t id;
		id = random_generate(&local->random) % cons->as_identity_modulo;
		value_set_int(identity, id);
	}
}

hot static inline Part*
row_map(Table* table, Value* refs, Value* values, Value* identity)
{
	// values are row columns
	auto mapping = &table->parts.mapping;
	auto hash_partition = value_hash_row(mapping->keys, refs, values, identity);
	hash_partition %= PART_MAPPING_MAX;
	return mapping->map[hash_partition];
}

hot static inline Part*
row_map_keys(Table* table, Value* keys)
{
	// values are row keys
	auto mapping = &table->parts.mapping;
	auto hash_partition = value_hash_keys(mapping->keys, NULL, keys, NULL);
	hash_partition %= PART_MAPPING_MAX;
	return mapping->map[hash_partition];
}
