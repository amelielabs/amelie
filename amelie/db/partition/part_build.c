
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>

hot static void
part_build_index(PartBuild* self, Iterator* it)
{
	// build secondary index by iterating and indexating primary keys
	auto    index = part_find(self->part, &self->index->name, true);
	assert(index);

	auto    keys = index_keys(index);
	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	for (; iterator_has(it); iterator_next(it))
	{
		auto row = iterator_at(it);
		ref_create(key, row, keys);
		auto prev = index_ingest(index, key);
		if (unlikely(prev))
			error("index unique constraint violation");
	}
}

hot static void
part_build_column_add(PartBuild* self, Iterator* it)
{
	// build partition with a new column based on the other partition
	auto primary = part_primary(self->part_dest);
	auto keys = index_keys(primary);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row with a new column data
		auto row = row_alter_add(origin, &self->column->constraint.value);
		guard(row_free, row);

		uint8_t key_data[keys->key_size];
		auto    key = (Ref*)key_data;
		ref_create(key, row, keys);

		// update primary index
		index_ingest(primary, key);
		unguard();

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
	}
}

hot static void
part_build_column_drop(PartBuild* self, Iterator* it)
{
	// build partition with a new column based on the other partition
	auto primary = part_primary(self->part_dest);
	auto keys = index_keys(primary);
	for (; iterator_has(it); iterator_next(it))
	{
		auto origin = iterator_at(it);

		// allocate row based on original row without a column
		auto row = row_alter_drop(origin, self->column->order);
		guard(row_free, row);

		uint8_t key_data[keys->key_size];
		auto    key = (Ref*)key_data;
		ref_create(key, row, keys);

		// update primary index
		index_ingest(primary, key);
		unguard();

		// update secondary indexes
		part_ingest_secondary(self->part_dest, row);
	}
}

void
part_build(PartBuild* self)
{
	auto id = self->part->config->id;
	auto it = index_iterator(part_primary(self->part));
	guard(iterator_close, it);
	iterator_open(it, NULL);
	switch (self->type) {
	case PART_BUILD_INDEX:
		log("build %" PRIu64 ": create index", id);
		part_build_index(self, it);
		break;
	case PART_BUILD_COLUMN_ADD:
		log("build %" PRIu64 ": add column", id);
		part_build_column_add(self, it);
		break;
	case PART_BUILD_COLUMN_DROP:
		log("build %" PRIu64 ": drop column", id);
		part_build_column_drop(self, it);
		break;
	}
	log("build %" PRIu64 ": complete", id);
}
