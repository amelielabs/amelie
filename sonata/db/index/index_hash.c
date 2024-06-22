
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>

hot static void
log_if_commit(Log* self, LogOp* op)
{
	auto index = (IndexHash*)op->iface_arg;
	if (! index->hash.keys->primary)
		return;
	RowKey* prev;
	log_row_of(self, op, &prev);
	if (prev->row)
		row_free(prev->row);
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto index = (IndexHash*)op->iface_arg;
	RowKey* prev;
	auto key = log_row_of(self, op, &prev);
	if (prev->row)
		hash_set(&index->hash, prev, NULL);
	else
		hash_delete(&index->hash, key, NULL);
	if (op->cmd != LOG_DELETE && index->hash.keys->primary)
		row_free(key->row);
}

static LogIf log_if =
{
	.commit = log_if_commit,
	.abort  = log_if_abort
};

hot static bool
index_hash_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_hash_of(arg);
	auto keys = self->hash.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	RowKey* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	row_key_create(key, row, keys);

	// update tree
	auto exists = hash_set(&self->hash, key, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);

	return exists;
}

hot static void
index_hash_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = index_hash_of(arg);
	auto keys = self->hash.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	RowKey* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	row_key_create(key, row, keys);

	// replace
	auto hash_it = index_hash_iterator_of(it);
	hash_iterator_replace(&hash_it->iterator, key, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_hash_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = index_hash_of(arg);
	auto keys = self->hash.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_DELETE, &log_if, self, keys);

	// prepare key
	RowKey* prev;
	auto key = log_row_of(&trx->log, op, &prev);

	// delete by using current position
	auto hash_it = index_hash_iterator_of(it);
	memcpy(key, hash_it->iterator.current, keys->key_size);

	hash_iterator_delete(&hash_it->iterator, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_hash_delete_by(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_hash_of(arg);
	auto keys = self->hash.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_DELETE, &log_if, self, keys);

	// prepare key
	RowKey* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	row_key_create(key, row, keys);

	// delete by key
	if (! hash_delete(&self->hash, key, prev))
	{
		// does not exists
		log_truncate(&trx->log);
		return;
	}

	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_hash_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = index_hash_of(arg);
	auto keys = self->hash.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	auto key = log_row_of(&trx->log, op, NULL);
	row_key_create(key, row, keys);

	// insert or return iterator on existing position
	uint64_t pos = 0;	
	if (hash_get_or_set(&self->hash, key, &pos))
	{
		*it = index_hash_iterator_allocate(self);
		auto hash_it = index_hash_iterator_of(*it);
		hash_iterator_open_at(&hash_it->iterator, &self->hash, pos);
		log_truncate(&trx->log);
		return;
	}

	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);

	*it = NULL;
}

hot static bool
index_hash_ingest(Index* arg, Row* row)
{
	auto    self = index_hash_of(arg);
	auto    keys = self->hash.keys;
	uint8_t key_data[keys->key_size];
	auto    key = (RowKey*)key_data;
	row_key_create(key, row, keys);
	uint64_t pos = 0;
	return hash_get_or_set(&self->hash, key, &pos);
}

hot static Iterator*
index_hash_open(Index* arg, RowKey* key, bool start)
{
	auto self = index_hash_of(arg);
	auto it = index_hash_iterator_allocate(self);
	if (start)
		index_hash_iterator_open(it, key);
	return it;
}

static void
index_hash_free(Index* arg)
{
	auto self = index_hash_of(arg);
	hash_free(&self->hash);
	so_free(self);
}

Index*
index_hash_allocate(IndexConfig* config, uint64_t partition)
{
	auto self = (IndexHash*)so_malloc(sizeof(IndexHash));
	index_init(&self->index, config, partition);
	hash_init(&self->hash);
	guard(index_hash_free, self);

	auto iface = &self->index.iface;
	iface->set       = index_hash_set;
	iface->update    = index_hash_update;
	iface->delete    = index_hash_delete;
	iface->delete_by = index_hash_delete_by;
	iface->upsert    = index_hash_upsert;
	iface->ingest    = index_hash_ingest;
	iface->open      = index_hash_open;
	iface->free      = index_hash_free;

	hash_create(&self->hash, &config->keys);
	unguard();
	return &self->index;
}
