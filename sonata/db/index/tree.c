
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>

static void
tree_commit(LogOp* op, uint64_t lsn)
{
	auto self = (Tree*)op->arg;
	self->index.lsn = lsn;

	// free row or add row to the free list
	if (op->row.prev)
		row_free(op->row.prev);
}

static void
tree_set_abort(LogOp* op)
{
	auto self = (Tree*)op->arg;
	// replace back or remove
	if (op->row.prev)
		ttree_set(&self->tree, op->row.prev);
	else
		ttree_unset(&self->tree, op->row.row);
	row_free(op->row.row);
}

hot static bool
tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	auto prev = ttree_set(&self->tree, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->storage,
	        &self->index.config->def,
	        row, prev);

	// is replace
	return prev != NULL;
}

hot static void
tree_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// replace
	auto tree_it = tree_iterator_of(it);
	auto prev = ttree_iterator_replace(&tree_it->iterator, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->storage,
	        &self->index.config->def,
	        row, prev);
}

static void
tree_delete_abort(LogOp* op)
{
	auto self = (Tree*)op->arg;
	// prev always exists
	ttree_set(&self->tree, op->row.prev);
}

hot static void
tree_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by using current position
	auto tree_it = tree_iterator_of(it);
	auto prev = ttree_iterator_delete(&tree_it->iterator);

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        tree_commit,
	        tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->storage,
	        &self->index.config->def,
	        prev, prev);
}

hot static void
tree_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by key
	auto prev = ttree_unset(&self->tree, key);
	if (! prev)
	{
		// does not exists
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        tree_commit,
	        tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->storage,
	        &self->index.config->def,
	        key, prev);
}

hot static void
tree_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// insert or return iterator on existing position
	TtreePos pos;
	auto prev = ttree_set_or_get(&self->tree, row, &pos);
	if (prev)
	{
		*it = tree_iterator_allocate(self);
		auto tree_it = tree_iterator_of(*it);
		ttree_iterator_open_at(&tree_it->iterator, &self->tree, &pos);
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->storage,
	        &self->index.config->def,
	        row, NULL);

	*it = NULL;
}

hot static void
tree_ingest(Index* arg, Row* row)
{
	auto self = tree_of(arg);
	auto prev = ttree_set(&self->tree, row);
	assert(! prev);
}

hot static Iterator*
tree_open(Index* arg, Row* key, bool start)
{
	auto self = tree_of(arg);
	auto it = tree_iterator_allocate(self);
	if (start)
		tree_iterator_open(it, key);
	return it;
}

static void
tree_free(Index* arg)
{
	auto self = tree_of(arg);
	ttree_free(&self->tree);
	so_free(self);
}

Index*
tree_allocate(IndexConfig* config, uint64_t storage)
{
	Tree* self = so_malloc(sizeof(*self));
	ttree_init(&self->tree, 512, 256, &config->def);
	index_init(&self->index, config, storage);

	auto iface = &self->index.iface;
	iface->set       = tree_set;
	iface->update    = tree_update;
	iface->delete    = tree_delete;
	iface->delete_by = tree_delete_by;
	iface->upsert    = tree_upsert;
	iface->ingest    = tree_ingest;
	iface->open      = tree_open;
	iface->free      = tree_free;
	return &self->index;
}
