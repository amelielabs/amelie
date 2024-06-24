
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

hot static void
log_if_commit(Log* self, LogOp* op)
{
	auto index = (IndexTree*)op->iface_arg;
	if (! index->tree.keys->primary)
		return;
	Ref* prev;
	log_row_of(self, op, &prev);
	if (prev->row)
		row_free(prev->row);
}

static void
log_if_abort(Log* self, LogOp* op)
{
	auto index = (IndexTree*)op->iface_arg;
	Ref* prev;
	auto key = log_row_of(self, op, &prev);
	if (prev->row)
		tree_set(&index->tree, prev, NULL);
	else
		tree_unset(&index->tree, key, NULL);
	if (op->cmd != LOG_DELETE && index->tree.keys->primary)
		row_free(key->row);
}

static LogIf log_if =
{
	.commit = log_if_commit,
	.abort  = log_if_abort
};

hot static bool
index_tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_tree_of(arg);
	auto keys = self->tree.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	Ref* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	ref_create(key, row, keys);

	// update tree
	auto exists = tree_set(&self->tree, key, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);

	return exists;
}

hot static void
index_tree_update(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = index_tree_of(arg);
	auto keys = self->tree.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	Ref* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	ref_create(key, row, keys);

	// replace
	auto tree_it = index_tree_iterator_of(it);
	tree_iterator_replace(&tree_it->iterator, key, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_tree_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = index_tree_of(arg);
	auto keys = self->tree.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_DELETE, &log_if, self, keys);

	// prepare key
	Ref* prev;
	auto key = log_row_of(&trx->log, op, &prev);

	// delete by using current position
	auto tree_it = index_tree_iterator_of(it);
	memcpy(key, tree_it->iterator.current, keys->key_size);

	tree_iterator_delete(&tree_it->iterator, prev);
	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_tree_delete_by(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_tree_of(arg);
	auto keys = self->tree.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_DELETE, &log_if, self, keys);

	// prepare key
	Ref* prev;
	auto key = log_row_of(&trx->log, op, &prev);
	ref_create(key, row, keys);

	// delete by key
	if (! tree_unset(&self->tree, key, prev))
	{
		// does not exists
		log_truncate(&trx->log);
		return;
	}

	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);
}

hot static void
index_tree_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = index_tree_of(arg);
	auto keys = self->tree.keys;

	// add log record
	auto op = log_row(&trx->log, LOG_REPLACE, &log_if, self, keys);

	// prepare key
	auto key = log_row_of(&trx->log, op, NULL);
	ref_create(key, row, keys);

	// insert or return iterator on existing position
	TreePos pos;
	if (tree_set_or_get(&self->tree, key, &pos))
	{
		*it = index_tree_iterator_allocate(self);
		auto tree_it = index_tree_iterator_of(*it);
		tree_iterator_open_at(&tree_it->iterator, &self->tree, &pos);
		log_truncate(&trx->log);
		return;
	}

	if (self->index.config->primary)
		log_persist(&trx->log, op, arg->partition);

	*it = NULL;
}

hot static bool
index_tree_ingest(Index* arg, Row* row)
{
	auto    self = index_tree_of(arg);
	auto    keys = self->tree.keys;
	uint8_t key_data[keys->key_size];
	auto    key  = (Ref*)key_data;
	ref_create(key, row, keys);

	TreePos pos;
	return tree_set_or_get(&self->tree, key, &pos);
}

hot static Iterator*
index_tree_open(Index* arg, Ref* key, bool start)
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
