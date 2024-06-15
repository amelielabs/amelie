
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
hash_commit(LogOp* op)
{
	// free row or add row to the free list
	if (op->row.prev)
		row_free(op->row.prev);
}

static void
hash_set_abort(LogOp* op)
{
	auto self = (Hash*)op->arg;
	// replace back or remove
	if (op->row.prev)
		htt_set(&self->ht, op->row.prev);
	else
		htt_delete(&self->ht, op->row.row);
	row_free(op->row.row);
}

hot static bool
hash_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	auto prev = htt_set(&self->ht, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        hash_commit,
	        hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        &self->index.config->keys,
	        row, prev);

	// is replace
	return prev != NULL;
}

hot static void
hash_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// replace
	auto hash_it = hash_iterator_of(it);
	auto prev = htt_iterator_replace(&hash_it->iterator, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        hash_commit,
	        hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        &self->index.config->keys,
	        row, prev);
}

static void
hash_delete_abort(LogOp* op)
{
	auto self = (Hash*)op->arg;
	// prev always exists
	htt_set(&self->ht, op->row.prev);
}

hot static void
hash_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by using current position
	auto hash_it = hash_iterator_of(it);
	auto prev = htt_iterator_delete(&hash_it->iterator);

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        hash_commit,
	        hash_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        &self->index.config->keys,
	        prev, prev);
}

hot static void
hash_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by key
	auto prev = htt_delete(&self->ht, key);
	if (! prev)
	{
		// does not exists
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        hash_commit,
	        hash_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        &self->index.config->keys,
	        key, prev);
}

hot static void
hash_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = hash_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// insert or return iterator on existing position
	uint64_t pos = 0;	
	auto prev = htt_get_or_set(&self->ht, row, &pos);
	if (prev)
	{
		*it = hash_iterator_allocate(self);
		auto hash_it = hash_iterator_of(*it);
		htt_iterator_open_at(&hash_it->iterator, &self->ht, pos);
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        hash_commit,
	        hash_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        &self->index.config->keys,
	        row, NULL);

	*it = NULL;
}

hot static void
hash_ingest(Index* arg, Row* row)
{
	auto self = hash_of(arg);
	auto prev = htt_set(&self->ht, row);
	assert(! prev);
}

hot static Iterator*
hash_open(Index* arg, Row* key, bool start)
{
	auto self = hash_of(arg);
	auto it = hash_iterator_allocate(self);
	if (start)
		hash_iterator_open(it, key);
	return it;
}

static void
hash_free(Index* arg)
{
	auto self = hash_of(arg);
	htt_free(&self->ht);
	so_free(self);
}

Index*
hash_allocate(IndexConfig* config, uint64_t partition)
{
	Hash* self = so_malloc(sizeof(*self));
	index_init(&self->index, config, partition);
	htt_init(&self->ht);
	guard(hash_free, self);

	auto iface = &self->index.iface;
	iface->set       = hash_set;
	iface->update    = hash_update;
	iface->delete    = hash_delete;
	iface->delete_by = hash_delete_by;
	iface->upsert    = hash_upsert;
	iface->ingest    = hash_ingest;
	iface->open      = hash_open;
	iface->free      = hash_free;

	htt_create(&self->ht, &config->keys);
	unguard();
	return &self->index;
}
