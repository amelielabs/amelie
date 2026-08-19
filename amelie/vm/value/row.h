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
Row* row_delete(Part*, Timeline*, Columns*, Row*);

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
