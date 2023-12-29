
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>

Storage*
storage_allocate(StorageConfig* config, Mapping* map, Uuid* table)
{
	auto self = (Storage*)mn_malloc(sizeof(Storage));
	self->list_count = 0;
	self->table      = table;
	self->map        = map;
	self->config     = config;
	buf_init(&self->list_dump);
	mutex_init(&self->list_dump_lock);
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
	buf_free(&self->list_dump);
	mutex_free(&self->list_dump_lock);
	mn_free(self);
}

static void
storage_dump_list(Storage* self)
{
	mutex_lock(&self->list_dump_lock);
	guard(unlock, mutex_unlock, &self->list_dump_lock);

	auto buf = &self->list_dump;
	buf_reset(buf);

	// array
	encode_array(buf, self->list_count);
	if (! self->list_count)
		return;

	auto part = part_tree_min(&self->tree);
	while (part)
	{
		// map
		encode_map(buf, 2);

		// min
		encode_raw(buf, "min", 3);
		encode_integer(buf, part->min);

		// max
		encode_raw(buf, "max", 3);
		encode_integer(buf, part->max);

		part = part_tree_next(&self->tree, part);
	}
}

void
storage_open(Storage* self, List* indexes)
{
	// todo
	//   recover empty partitions
	//   bootstrap

	if  (self->map->type == MAP_RANGE)
	{
		storage_dump_list(self);
		return;
	}

	// allocate partition
	auto part = part_allocate(self->table, &self->config->id);
	list_append(&self->list, &part->link);
	self->list_count++;

	// add partition to the partition tree
	part_tree_add(&self->tree, part);
	storage_dump_list(self);

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

	// create new partition

	// allocate partition
	auto part = part_allocate(self->table, &self->config->id);
	list_append(&self->list, &part->link);
	self->list_count++;

	if (key >= 0)
	{
		int64_t min = key - (key % self->map->interval);
		part->min = min;
		part->max = min + self->map->interval;
	} else
	{
		key = -key;
		int64_t min = key - (key % self->map->interval);
		part->min =  min + self->map->interval;
		part->max =  min;
		part->min = -part->min;
		part->max = -part->max;
	}

	// add partition to the partition tree
	part_tree_add(&self->tree, part);
	storage_dump_list(self);

	*created = true;
	return part;
}
