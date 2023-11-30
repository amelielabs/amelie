
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>

Storage*
storage_allocate(StorageConfig* config, Mapping* map, Uuid* table)
{
	auto self = (Storage*)mn_malloc(sizeof(Part));
	self->list_count = 0;
	self->table      = table;
	self->map        = map;
	self->config     = config;
	list_init(&self->list);
	list_init(&self->link);
	part_tree_init(&self->tree);
	return self;
}

void
storage_free(Storage* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	mn_free(self);
}

void
storage_open(Storage* self, List* indexes)
{
	// todo
	//   recover empty partitions
	//   bootstrap

	if  (self->map->type == MAP_RANGE ||
	     self->map->type == MAP_RANGE_AUTO)
		return;

	// allocate partition
	auto part = part_allocate(self->table, &self->config->id);
	list_append(&self->list, &part->link);
	self->list_count++;

	// add partition to the partition tree
	part_tree_add(&self->tree, part);

	// create indexes
	part_open(part, indexes);
}

hot static inline int64_t
storage_map_key(Def* def, uint8_t* data)
{
	auto key = def->key;
	auto column = def_column_of(def, key->column);

	// validate row and rewind to the column
	column_find(column, &data);

	// find key by path
	key_find(column, key, &data);

	// read key value
	int64_t value;
	data_read_integer(&data, &value);
	return value;
}

hot Part*
storage_map(Storage* self, Def* def, uint8_t* data, int data_size,
            bool* created)
{
	unused(data_size);

	// reference or non-partitioned sharding
	if (self->map->type == MAP_NONE ||
	    self->map->type == MAP_REFERENCE)
		return container_of(self->list.next, Part, link);

	// MAP_RANGE
	// MAP_RANGE_AUTO

	// read partition key
	auto key = storage_map_key(def, data);

	// find matching partition
	if (likely(self->list_count > 0))
	{
		// validate partition range
		auto part = part_tree_match(&self->tree, key);
		if (likely(key >= part->min && key < part->max))
			return part;
	}

	// partition must exists for declarative partitioning
	if (self->map->type == MAP_RANGE)
		error("partition for key '%" PRIu64 "' does not exists", key);

	// create new partition

	// allocate partition
	auto part = part_allocate(self->table, &self->config->id);
	list_append(&self->list, &part->link);
	self->list_count++;

	int64_t min = key - (key % self->map->interval);
	part->min = min;
	part->max = min + self->map->interval;

	// add partition to the partition tree
	part_tree_add(&self->tree, part);

	*created = true;
	return part;
}
