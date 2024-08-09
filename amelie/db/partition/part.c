
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

Part*
part_allocate(PartConfig* config, Serial* serial)
{
	auto self = (Part*)am_malloc(sizeof(Part));
	self->route         = NULL;
	self->config        = NULL;
	self->indexes_count = 0;
	self->serial        = serial;
	list_init(&self->indexes);
	list_init(&self->link_cp);
	list_init(&self->link);
	hashtable_node_init(&self->link_ht);
	guard(part_free, self);
	self->config = part_config_copy(config);
	unguard();
	return self;
}

void
part_free(Part* self)
{
	list_foreach_safe(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_free(index);
	}
	part_config_free(self->config);
	am_free(self);
}

void
part_index_create(Part* self, IndexConfig* config)
{
	Index* index;
	if (config->type == INDEX_TREE)
		index = index_tree_allocate(config);
	else
	if (config->type == INDEX_HASH)
		index = index_hash_allocate(config);
	else
		error("unrecognized index type");
	list_append(&self->indexes, &index->link);
	self->indexes_count++;
}

void
part_index_drop(Part* self, IndexConfig* config)
{
	auto index = part_find(self, &config->name, true);
	list_unlink(&index->link);
	self->indexes_count--;
	index_free(index);
}

void
part_truncate(Part* self)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		index_truncate(index);
	}
}

Index*
part_find(Part* self, Str* name, bool error_if_not_exists)
{
	list_foreach(&self->indexes)
	{
		auto index = list_at(Index, link);
		if (str_compare(&index->config->name, name))
			return index;
	}
	if (error_if_not_exists)
		error("index '%.*s': not exists", str_size(name),
		       str_of(name));
	return NULL;
}
