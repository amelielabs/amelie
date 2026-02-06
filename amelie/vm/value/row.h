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

Row* row_create(Heap*, Columns*, Value*, Value*, Value*);
Row* row_create_key(Buf*, Keys*, Value*, int);
Row* row_update(Heap*, Row*, Columns*, Value*, int);

static inline void
row_get_identity(Table* table, Value* refs, Value* row, Value* identity)
{
	auto columns = table_columns(table);
	if (! columns->identity)
	{
		value_set_int(identity, 0);
		return;
	}

	// get identity column value
	auto value = row + columns->identity->order;
	if (value->type == TYPE_REF)
		value = &refs[value->integer];
	if (value->type != TYPE_NULL)
	{
		value_set_int(identity, value->integer);
		return;
	}

	// generate identity column value
	auto cons = &columns->identity->constraints;
	uint64_t id;
	if (cons->as_identity == IDENTITY_SERIAL)
		id = sequence_next(&table->seq);
	else
		id = random_generate(&runtime()->random) % cons->as_identity_modulo;
	value_set_int(identity, id);
}

hot static inline Part*
row_map(Table* table, Value* refs, Value* values, Value* identity)
{
	// values are row columns
	auto mapping = &table->engine.mapping;
	auto hash_partition = value_hash_row(mapping->keys, refs, values, identity);
	hash_partition %= PART_MAPPING_MAX;
	return mapping->map[hash_partition];
}

hot static inline Part*
row_map_keys(Table* table, Value* values)
{
	// values are row keys
	auto mapping = &table->engine.mapping;
	auto hash_partition = value_hash_keys(mapping->keys, NULL, values, 0);
	hash_partition %= PART_MAPPING_MAX;
	return mapping->map[hash_partition];
}
