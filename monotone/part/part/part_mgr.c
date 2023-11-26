
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
	handle_mgr_init(&self->mgr);
}

void
part_mgr_free(PartMgr* self)
{
	handle_mgr_free(&self->mgr);
}

void
part_mgr_open(PartMgr* self, CatalogMgr* cat_mgr)
{
	// register catalog
	auto cat = catalog_allocate("partitions", &part_catalog_if, self);
	catalog_mgr_add(cat_mgr, cat);
}

Part*
part_mgr_create(PartMgr* self, Transaction* trx, PartConfig* config)
{
	// allocate partition
	auto part = part_allocate(config);
	guard(guard, part_free, part);

	// save create partition operation
	auto op = part_op_create(config);

	// update partitions
	handle_mgr_write(&self->mgr, trx, LOG_PART_CREATE, &part->handle, op);

	buf_unpin(op);
	unguard(&guard);
	return part;
}

static void
part_mgr_drop(PartMgr* self, Transaction* trx, Part* part)
{
	// save drop partition operation
	auto op = part_op_drop(&part->config->id);

	// drop partition by id
	Handle drop;
	handle_init(&drop);
	handle_set_id(&drop, &part->config->id);

	handle_mgr_write(&self->mgr, trx, LOG_PART_DROP, &drop, op);
	buf_unpin(op);
}

void
part_mgr_drop_by_id(PartMgr* self, Transaction* trx, Uuid* id)
{
	list_foreach_safe(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));
		if (uuid_compare(&part->config->id, id))
		{
			part_mgr_drop(self, trx, part);
			break;
		}
	}
}

void
part_mgr_drop_by_id_table(PartMgr* self, Transaction* trx, Uuid* id)
{
	list_foreach_safe(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));
		if (uuid_compare(&part->config->id_table, id))
			part_mgr_drop(self, trx, part);
	}
}

Buf*
part_mgr_list(PartMgr* self)
{
	auto buf = msg_create(MSG_OBJECT);

	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));

		// map
		encode_map(buf, 2);

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
	list_foreach(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));
		if (uuid_compare(&part->config->id, id))
			return part;
	}
	return NULL;
}

Part*
part_mgr_find_for(PartMgr* self, Uuid* shard, Uuid* table)
{
	list_foreach(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));
		if (uuid_compare(&part->config->id_shard, shard) &&
		    uuid_compare(&part->config->id_table, table))
			return part;
	}
	return NULL;
}

Part*
part_mgr_find_for_table(PartMgr* self, Uuid* table)
{
	list_foreach(&self->mgr.list)
	{
		auto part = part_of(list_at(Handle, link));
		if (uuid_compare(&part->config->id_table, table))
			return part;
	}
	return NULL;
}

void
part_mgr_dump(PartMgr* self, Buf* buf)
{
	// array
	encode_array(buf, self->mgr.list_count);
	list_foreach(&self->mgr.list)
	{
		// array
		encode_array(buf, 2);

		// config
		auto part = part_of(list_at(Handle, link));
		part_config_write(part->config, buf);

		// indexes
		encode_array(buf, part->storage.indexes_count);
		list_foreach(&part->storage.indexes)
		{
			auto index = list_at(Index, link);
			index_config_write(index->config, buf);
		}
	}
}
