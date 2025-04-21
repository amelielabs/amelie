
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

hot static uint64_t
part_build_index(PartBuild* self, Iterator* it)
{
	// build secondary index by iterating and indexating primary keys
	uint64_t count = 0;
	auto index = part_find(self->part, &self->index->name, true);
	for (; iterator_has(it); iterator_next(it))
	{
		auto row = iterator_at(it);
		auto prev = index_replace_by(index, row);
		if (unlikely(prev))
			error("index unique constraint violation");
		count++;
	}
	return count;
}

hot static uint64_t
part_build_column_add(PartBuild* self, Iterator* it)
{
	// build partition with a new column based on the other partition
	uint64_t count = 0;
	auto primary = part_primary(self->part);
	auto primary_columns = index_keys(primary)->columns;
	auto primary_dest = part_primary(self->part_dest);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row with a new column
		auto row = row_alter_add(&self->part_dest->heap, origin, primary_columns);

		// update primary index
		index_replace_by(primary_dest, row);

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
		count++;
	}
	return count;
}

hot static uint64_t
part_build_column_drop(PartBuild* self, Iterator* it)
{
	// build partition without a column based on the other partition
	uint64_t count = 0;
	auto primary = part_primary(self->part);
	auto primary_columns = index_keys(primary)->columns;
	auto primary_dest = part_primary(self->part_dest);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row without a column
		auto row = row_alter_drop(&self->part_dest->heap, origin, primary_columns, self->column);

		// update primary index
		index_replace_by(primary_dest, row);

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
		count++;
	}
	return count;
}

void
part_build(PartBuild* self)
{
	auto id = self->part->config->id;
	auto it = index_iterator(part_primary(self->part));
	defer(iterator_close, it);
	iterator_open(it, NULL);
	uint64_t count;
	switch (self->type) {
	case PART_BUILD_INDEX:
		count = part_build_index(self, it);
		break;
	case PART_BUILD_COLUMN_ADD:
		count = part_build_column_add(self, it);
		break;
	case PART_BUILD_COLUMN_DROP:
		count = part_build_column_drop(self, it);
		break;
	}
	info("partition %" PRIu64 " (%" PRIu64 " rows processed)",
	     id, count);
}
