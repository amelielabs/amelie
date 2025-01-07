
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

hot static void
part_build_index(PartBuild* self, Iterator* it)
{
	// build secondary index by iterating and indexating primary keys
	auto index = part_find(self->part, &self->index->name, true);
	for (; iterator_has(it); iterator_next(it))
	{
		auto row = iterator_at(it);
		auto prev = index_ingest(index, row);
		if (unlikely(prev))
			error("index unique constraint violation");
	}
}

hot static void
part_build_column_add(PartBuild* self, Iterator* it)
{
	// build partition with a new column based on the other partition
	auto primary = part_primary(self->part);
	auto primary_columns = index_keys(primary)->columns;
	auto primary_dest = part_primary(self->part_dest);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row with a new column
		auto row = row_alter_add(origin, primary_columns);

		// update primary index
		index_ingest(primary_dest, row);

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
	}
}

hot static void
part_build_column_drop(PartBuild* self, Iterator* it)
{
	// build partition without a column based on the other partition
	auto primary = part_primary(self->part);
	auto primary_columns = index_keys(primary)->columns;
	auto primary_dest = part_primary(self->part_dest);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row without a column
		auto row = row_alter_drop(origin, primary_columns, self->column);

		// update primary index
		index_ingest(primary_dest, row);

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
	}
}

void
part_build(PartBuild* self)
{
	auto id = self->part->config->id;
	auto it = index_iterator(part_primary(self->part));
	defer(iterator_close, it);
	iterator_open(it, NULL);
	switch (self->type) {
	case PART_BUILD_INDEX:
		info("build %.*s.%.*s: create index %.*s (partition %" PRIu64 ")",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->index->name),
		     str_of(&self->index->name),
		     id);
		part_build_index(self, it);
		info("build %.*s.%.*s: create index %.*s (partition %" PRIu64 ") complete",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->index->name),
		     str_of(&self->index->name),
		     id);
		break;
	case PART_BUILD_COLUMN_ADD:
		info("build %.*s.%.*s: add column %.*s (partition %" PRIu64 ")",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name),
		     id);
		part_build_column_add(self, it);
		info("build %.*s.%.*s: add column %.*s (partition %" PRIu64 ") complete",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name),
		     id);
		break;
	case PART_BUILD_COLUMN_DROP:
		info("build %.*s.%.*s: drop column %.*s (partition %" PRIu64 ")",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name),
		     id);
		part_build_column_drop(self, it);
		info("build %.*s.%.*s: drop column %.*s (partition %" PRIu64 ") complete",
		     str_size(self->schema),
		     str_of(self->schema),
		     str_size(self->name),
		     str_of(self->name),
		     str_size(&self->column->name),
		     str_of(&self->column->name),
		     id);
		break;
	}
}
