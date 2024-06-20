
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

static void
index_hash_commit(LogOp* op)
{
	// free row or add row to the free list
	auto self = (IndexHash*)op->arg;
	if (self->hash.keys->primary && op->row.prev)
		row_free(op->row.prev);
}

static void
index_hash_set_abort(LogOp* op)
{
	auto self = (IndexHash*)op->arg;
	// replace back or remove
	if (op->row.prev)
		hash_set(&self->hash, op->row.prev);
	else
		hash_delete(&self->hash, op->row.row);
	if (self->hash.keys->primary)
		row_free(op->row.row);
}

hot static bool
index_hash_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	auto prev = hash_set(&self->hash, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_hash_commit,
	        index_hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, prev);

	// is replace
	return prev != NULL;
}

hot static void
index_hash_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = index_hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// replace
	auto hash_it = index_hash_iterator_of(it);
	auto prev = hash_iterator_replace(&hash_it->iterator, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_hash_commit,
	        index_hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, prev);
}

static void
index_hash_delete_abort(LogOp* op)
{
	auto self = (IndexHash*)op->arg;
	// prev always exists
	hash_set(&self->hash, op->row.prev);
}

hot static void
index_hash_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = index_hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by using current position
	auto hash_it = index_hash_iterator_of(it);
	auto prev = hash_iterator_delete(&hash_it->iterator);

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        index_hash_commit,
	        index_hash_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        prev, prev);
}

hot static void
index_hash_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = index_hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by key
	auto prev = hash_delete(&self->hash, key);
	if (! prev)
	{
		// does not exists
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        index_hash_commit,
	        index_hash_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        key, prev);
}

hot static void
index_hash_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = index_hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// insert or return iterator on existing position
	uint64_t pos = 0;	
	auto prev = hash_get_or_set(&self->hash, row, &pos);
	if (prev)
	{
		*it = index_hash_iterator_allocate(self);
		auto hash_it = index_hash_iterator_of(*it);
		hash_iterator_open_at(&hash_it->iterator, &self->hash, pos);
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_hash_commit,
	        index_hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, NULL);

	*it = NULL;
}

hot static Row*
index_hash_ingest(Index* arg, Row* row)
{
	auto self = index_hash_of(arg);
	uint64_t pos = 0;
	return hash_get_or_set(&self->hash, row, &pos);
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
