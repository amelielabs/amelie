
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

always_inline static inline IndexTree*
index_tree_of(Index* self)
{
	return (IndexTree*)self;
}

static void
index_tree_commit(LogOp* op)
{
	// free row or add row to the free list
	auto self = (IndexTree*)op->arg;
	if (self->tree.keys->primary && op->row.prev)
		row_free(op->row.prev);
}

static void
index_tree_set_abort(LogOp* op)
{
	auto self = (IndexTree*)op->arg;
	// replace back or remove
	if (op->row.prev)
		tree_set(&self->tree, op->row.prev);
	else
		tree_unset(&self->tree, op->row.row);
	if (self->tree.keys->primary)
		row_free(op->row.row);
}

hot static bool
index_tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	auto prev = tree_set(&self->tree, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_tree_commit,
	        index_tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, prev);

	// is replace
	return prev != NULL;
}

hot static void
index_tree_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// replace
	auto tree_it = index_tree_iterator_of(it);
	auto prev = tree_iterator_replace(&tree_it->iterator, row);

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_tree_commit,
	        index_tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, prev);
}

static void
index_tree_delete_abort(LogOp* op)
{
	auto self = (IndexTree*)op->arg;
	// prev always exists
	tree_set(&self->tree, op->row.prev);
}

hot static void
index_tree_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by using current position
	auto tree_it = index_tree_iterator_of(it);
	auto prev = tree_iterator_delete(&tree_it->iterator);

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        index_tree_commit,
	        index_tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        prev, prev);
}

hot static void
index_tree_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// delete by key
	auto prev = tree_unset(&self->tree, key);
	if (! prev)
	{
		// does not exists
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        index_tree_commit,
	        index_tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        key, prev);
}

hot static void
index_tree_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log);

	// insert or return iterator on existing position
	TreePos pos;
	auto prev = tree_set_or_get(&self->tree, row, &pos);
	if (prev)
	{
		*it = index_tree_iterator_allocate(self);
		auto tree_it = index_tree_iterator_of(*it);
		tree_iterator_open_at(&tree_it->iterator, &self->tree, &pos);
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        index_tree_commit,
	        index_tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->partition,
	        row, NULL);

	*it = NULL;
}

hot static Row*
index_tree_ingest(Index* arg, Row* row)
{
	auto self = index_tree_of(arg);
	TreePos pos;
	return tree_set_or_get(&self->tree, row, &pos);
}

hot static Iterator*
index_tree_open(Index* arg, RowKey* key, bool start)
{
	auto self = index_tree_of(arg);
	auto it = index_tree_iterator_allocate(self);
	if (start)
		index_tree_iterator_open(it, key);
	return it;
}

static void
index_tree_free(Index* arg)
{
	auto self = index_tree_of(arg);
	tree_free(&self->tree);
	so_free(self);
}

Index*
index_tree_allocate(IndexConfig* config, uint64_t partition)
{
	auto self = (IndexTree*)so_malloc(sizeof(IndexTree));
	index_init(&self->index, config, partition);
	tree_init(&self->tree, 512, 256, &config->keys);

	auto iface = &self->index.iface;
	iface->set       = index_tree_set;
	iface->update    = index_tree_update;
	iface->delete    = index_tree_delete;
	iface->delete_by = index_tree_delete_by;
	iface->upsert    = index_tree_upsert;
	iface->ingest    = index_tree_ingest;
	iface->open      = index_tree_open;
	iface->free      = index_tree_free;
	return &self->index;
}
