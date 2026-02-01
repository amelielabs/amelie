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

hot static inline Part*
row_map(Table* table, Value* refs, Value* values, int64_t identity)
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
