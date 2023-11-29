
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
storage_allocate(StorageConfig* config, StorageMap* map, Uuid* table)
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
	
	// allocate partition
	auto part = part_allocate(self->table, &self->config->id, 0);
	list_append(&self->list, &part->link);
	self->list_count++;

	// add partition to the partition tree
	part_tree_add(&self->tree, part);

	// create indexes
	part_open(part, indexes);
}

hot Part*
storage_map(Storage* self, uint8_t* data, int data_size, bool* created)
{
	if (self->map->type == MAP_NONE ||
	    self->map->type == MAP_SHARD)
		return container_of(self->list.next, Part, link);

	// MAP_SHARD_RANGE
	// MAP_SHARD_RANGE_AUTO

	// todo: get first key value from data
	uint64_t key = 0;
	(void)key;
	(void)created;

	// todo:
	//  - get partition key (use Def first key)
	//  - find or create new partition
	//  - create empty new parition without indexes
	//
	(void)data;
	(void)data_size;

	return container_of(self->list.next, Part, link);
}
