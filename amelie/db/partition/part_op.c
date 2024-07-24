
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

static Ref*
log_if_rollback(Log* self, LogOp* op)
{
	auto index = (Index*)op->iface_arg;
	Ref* prev;
	auto key = log_row_of(self, op, &prev);
	if (prev->row)
		index_set(index, prev, NULL);
	else
	if (key->row)
		index_delete_by(index, key, NULL);
	return key;
}

hot static void
log_if_commit(Log* self, LogOp* op)
{
	Ref* prev;
	log_row_of(self, op, &prev);
	if (prev->row)
		row_free(prev->row);
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto key = log_if_rollback(self, op);
	if (op->cmd != LOG_DELETE && key->row)
		row_free(key->row);
}

hot static void
log_if_secondary_commit(Log* self, LogOp* op)
{
	unused(self);
	unused(op);
	// do nothing
}

static void
log_if_secondary_abort(Log* self, LogOp* op)
{
	log_if_rollback(self, op);
}

static LogIf log_if =
{
	.commit = log_if_commit,
	.abort  = log_if_abort
};

static LogIf log_if_secondary =
{
	.commit = log_if_secondary_commit,
	.abort  = log_if_secondary_abort
};

hot void
part_insert(Part*        self,
            Transaction* trx,
            bool         replace,
            uint8_t**    pos)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate primary row
	auto row = row_create(keys->columns, pos);
	guard(row_free, row);

	// add log record
	Ref* key, *prev;
	log_row(&trx->log, LOG_REPLACE, &log_if, primary, keys, &key, &prev);
	ref_create(key, row, keys);
	unguard();
	log_persist(&trx->log, self->config->id);

	// update primary index
	auto exists = index_set(primary, key, prev);
	if (unlikely(exists && !replace))
		error("index '%.*s': unique key constraint violation",
		      str_size(&primary->config->name),
		      str_of(&primary->config->name));

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		log_row(&trx->log, LOG_REPLACE, &log_if_secondary, index, keys, &key, &prev);
		ref_create(key, row, keys);

		auto exists = index_set(index, key, prev);
		if (unlikely(exists && !replace))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}
}

hot void
part_update(Part*        self,
            Transaction* trx,
            Iterator*    it,
            uint8_t**    pos)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate row
	auto row = row_create(keys->columns, pos);
	guard(row_free, row);

	// add log record
	Ref* key, *prev;
	log_row(&trx->log, LOG_REPLACE, &log_if, primary, keys, &key, &prev);
	ref_create(key, row, keys);
	unguard();
	log_persist(&trx->log, self->config->id);

	// update primary index
	index_update(primary, key, prev, it);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		log_row(&trx->log, LOG_REPLACE, &log_if_secondary, index, keys, &key, &prev);
		ref_create(key, row, keys);

		// find and replace existing secondary row (keys are not updated)
		auto index_it = index_iterator(index);
		guard(iterator_close, index_it);
		iterator_open(index_it, key);
		index_update(index, key, prev, index_it);
	}
}

hot void
part_delete(Part*        self,
            Transaction* trx,
            Iterator*    it)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);
	auto row = iterator_at(it);

	// add log record
	Ref* key, *prev;
	log_row(&trx->log, LOG_DELETE, &log_if, primary, keys, &key, &prev);

	// update primary index
	index_delete(primary, key, prev, it);
	log_persist(&trx->log, self->config->id);

	// secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		log_row(&trx->log, LOG_DELETE, &log_if_secondary, index, keys, &key, &prev);
		ref_create(key, row, keys);

		// delete by key
		index_delete_by(index, key, prev);
	}
}

hot void
part_delete_by(Part*        self,
               Transaction* trx,
               uint8_t**    pos)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate row to use as a key
	auto row = row_create(keys->columns, pos);
	guard(row_free, row);

	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	ref_create(key, row, keys);

	auto it = index_iterator(primary);
	guard(iterator_close, it);
	if (unlikely(! iterator_open(it, key)))
		error("delete by key does not match");

	part_delete(self, trx, it);
}

hot bool
part_upsert(Part*        self,
            Transaction* trx,
            Iterator*    it,
            uint8_t**    pos)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate primary row
	auto row = row_create(primary->config->keys.columns, pos);
	guard(row_free, row);

	// add log record
	Ref* key, *prev;
	log_row(&trx->log, LOG_REPLACE, &log_if, primary, keys, &key, &prev);
	ref_create(key, row, keys);
	unguard();

	// insert or get (iterator is openned in both cases)
	auto exists = index_upsert(primary, key, it);
	if (exists)
	{
		row_free(row);
		log_truncate(&trx->log);
		return true;
	}

	// insert
	log_persist(&trx->log, self->config->id);

	// update secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto index = list_at(Index, link);
		auto keys = index_keys(index);

		// add log record
		log_row(&trx->log, LOG_REPLACE, &log_if_secondary, index, keys, &key, &prev);
		ref_create(key, row, keys);

		auto exists = index_set(index, key, prev);
		if (unlikely(exists))
			error("index '%.*s': unique key constraint violation",
			      str_size(&index->config->name),
			      str_of(&index->config->name));
	}
	return false;
}

hot void
part_ingest_secondary(Part* self, Row* row)
{
	// update secondary indexes
	auto primary = part_primary(self);
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto    index = list_at(Index, link);
		auto    keys = index_keys(index);
		uint8_t key_data[keys->key_size];
		auto    key = (Ref*)key_data;
		ref_create(key, row, keys);
		index_ingest(index, key);
	}
}

hot void
part_ingest(Part* self, uint8_t** pos)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate row
	auto row = row_create(keys->columns, pos);
	guard(row_free, row);

	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	ref_create(key, row, keys);

	// update primary index
	index_ingest(primary, key);
	unguard();

	// secondary indexes
	part_ingest_secondary(self, row);
}

/*
void
part_ingest_alter_add(Part* self, Row* origin, Buf* data)
{
	auto primary = part_primary(self);
	auto keys = index_keys(primary);

	// allocate row based on original row with a new column data
	auto row = row_alter_add(origin, data);
	guard(row_free, row);

	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	ref_create(key, row, keys);

	// update primary index
	index_ingest(primary, key);
	unguard();

	// secondary indexes
	list_foreach_after(&self->indexes, &primary->link)
	{
		auto    index = list_at(Index, link);
		auto    keys = index_keys(index);
		uint8_t key_data[keys->key_size];
		auto    key = (Ref*)key_data;
		ref_create(key, row, keys);
		index_ingest(index, key);
	}
}
*/
