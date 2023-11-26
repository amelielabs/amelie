
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
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_part.h>

void
part_mgr_init(PartMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

void
part_mgr_free(PartMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		part_free(part);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static void
part_mgr_save(PartMgr* self)
{
	// dump a list of parts
	auto dump = buf_create(0);
	encode_array(dump, self->list_count);
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);

		// array
		encode_array(dump, 2);

		// part config
		part_config_write(part->config, dump);

		// indexes
		encode_array(dump, part->storage.indexes_count);
		list_foreach(&part->storage.indexes)
		{
			auto index = list_at(Index, link);
			index_config_write(index->config, dump);
		}
	}

	// update and save state
	var_data_set_buf(&config()->parts, dump);
	control_save_config();
	buf_free(dump);
}

static void
part_mgr_recover(PartMgr* self)
{
	auto parts = &config()->parts;
	if (! var_data_is_set(parts))
		return;
	auto pos = var_data_of(parts);
	if (data_is_null(pos))
		return;

	// array
	int count;
	data_read_array(&pos, &count);
	for (int i = 0; i < count; i++)
	{
		// array
		int count;
		data_read_array(&pos, &count);

		// part config
		auto config = part_config_read(&pos);
		guard(config_guard, part_config_free, config);

		// prepare part
		auto part = part_allocate(config);
		list_append(&self->list, &part->link);
		self->list_count++;

		// indexes
		data_read_array(&pos, &count);
		for (int i = 0; i < count; i++)
		{
			auto index_config = index_config_read(&pos);
			guard(config_index_guard, index_config_free, index_config);

			// todo: chose by type
			auto index = tree_allocate(index_config, &part->config->id);
			storage_attach(&part->storage, index);
		}

		// reusing config
		unguard(&config_guard);
	}
}

void
part_mgr_open(PartMgr* self)
{
	// recover parts objects
	part_mgr_recover(self);
}

void
part_mgr_gc(PartMgr* self)
{
	bool updated = false;
	list_foreach_safe(&self->list)
	{
		auto part = list_at(Part, link);
		if (part->refs > 0)
			continue;
		list_unlink(&part->link);
		self->list_count--;
		part_free(part);
		updated = true;
	}

	if (updated)
		part_mgr_save(self);
}

Buf*
part_mgr_list(PartMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);

	// array
	encode_array(buf, self->list_count);
	list_foreach(&self->list)
	{
		// {}
		auto part = list_at(Part, link);

		// map
		encode_map(buf, 3);

		// refs
		encode_raw(buf, "refs", 3);
		encode_integer(buf, part->refs);

		// config
		encode_raw(buf, "id", 2);
		part_config_write(part->config, buf);

		// index[]
		encode_raw(buf, "index", 5);
		encode_array(buf, part->storage.indexes_count);
		list_foreach(&part->storage.indexes)
		{
			auto index = list_at(Index, link);
			index_config_write(index->config, buf);
		}
	}

	msg_end(buf);
	return buf;
}

Part*
part_mgr_find(PartMgr* self, Uuid* id)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (uuid_compare(&part->config->id, id))
			return part;
	}
	return NULL;
}

Part*
part_mgr_find_for(PartMgr* self, Uuid* shard, Uuid* table)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (uuid_compare(&part->config->id_shard, shard) &&
		    uuid_compare(&part->config->id_table, table))
			return part;
	}
	return NULL;
}

Part*
part_mgr_find_for_table(PartMgr* self, Uuid* table)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link);
		if (uuid_compare(&part->config->id_table, table))
			return part;
	}
	return NULL;
}

Part*
part_mgr_create(PartMgr* self, PartConfig* config)
{
	auto part = part_allocate(config);
	list_append(&self->list, &part->link);
	self->list_count++;

	part_mgr_save(self);
	return part;
}

void
part_mgr_attach(PartMgr* self, Part* part, Index* index)
{
	storage_attach(&part->storage, index);
	part_mgr_save(self);
}

void
part_mgr_detach(PartMgr* self, Part* part, Index* index)
{
	storage_detach(&part->storage, index);
	part_mgr_save(self);
}
