
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
#include <monotone_index.h>

rbtree_free(tree_truncate, row_free(tree_row_of(n)))

static void
tree_free(Index* arg)
{
	auto self = tree_of(arg);
	if (self->tree.root)
		tree_truncate(self->tree.root, NULL);
	rbtree_init(&self->tree);
	self->tree_count = 0;
	mn_free(self);
}

hot rbtree_get(tree_find, compare(arg, tree_row_of(n), key));

static void
tree_commit(LogOp* op, uint64_t lsn)
{
	auto self = (Tree*)op->arg;
	self->lsn = lsn;
	if (op->row.prev)
		row_free(op->row.prev);
}

static void
tree_set_abort(LogOp* op)
{
	auto self = (Tree*)op->arg;
	TreeRow* ref = row_reserved(op->row.row);
	if (op->row.prev)
	{
		// abort replace
		TreeRow* ref_prev = row_reserved(op->row.prev);
		rbtree_replace(&self->tree, &ref->node, &ref_prev->node);
	} else
	{
		// abort insert
		rbtree_remove(&self->tree, &ref->node);
		self->tree_count--;
	}
	row_free(op->row.row);
}

hot static bool
tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_REPLACE, arg->id_table, arg->id_storage);

	TreeRow* ref = row_reserved(row);
	rbtree_init_node(&ref->node);

	// find existing row
	Row* prev = NULL;
	RbtreeNode* node;
	int rc;
	rc = tree_find(&self->tree, &self->index.config->def, row, &node);
	if (rc == 0 && node)
	{
		// replace
		prev = tree_row_of(node);
		TreeRow* ref_prev = row_reserved(prev);
		rbtree_replace(&self->tree, &ref_prev->node, &ref->node);
	} else
	{
		// insert
		rbtree_set(&self->tree, node, rc, &ref->node);
		self->tree_count++;
	}

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->id_table,
	        arg->id_storage,
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
	log_reserve(&trx->log, LOG_REPLACE, arg->id_table, arg->id_storage);

	// replace
	auto prev = iterator_at(it);
	assert(prev);
	TreeRow* ref      = row_reserved(row);
	TreeRow* ref_prev = row_reserved(prev);
	rbtree_replace(&self->tree, &ref_prev->node, &ref->node);

	// update iterator
	auto tree_it = tree_iterator_of(it);
	tree_it->current = row;
	tree_it->pos = &ref->node;

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->id_table,
	        arg->id_storage,
	        &self->index.config->def,
	        row, prev);
}

static void
tree_delete_abort(LogOp* op)
{
	auto self = (Tree*)op->arg;
	RbtreeNode* node;
	int rc = tree_find(&self->tree, &self->index.config->def, op->row.prev, &node);
	TreeRow* ref_prev = row_reserved(op->row.prev);
	rbtree_init_node(&ref_prev->node);
	rbtree_set(&self->tree, node, rc, &ref_prev->node);
	self->tree_count++;

	// delete row is not allocated
}

hot static void
tree_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_DELETE, arg->id_table, arg->id_storage);

	// update iterator next pos before removal
	auto tree_it = tree_iterator_of(it);
	tree_it->pos_next = rbtree_next(&self->tree, tree_it->pos);

	// delete
	auto prev = iterator_at(it);
	assert(prev);
	TreeRow* ref_prev = row_reserved(prev);
	rbtree_remove(&self->tree, &ref_prev->node);
	self->tree_count--;

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        tree_commit,
	        tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->id_table,
	        arg->id_storage,
	        &self->index.config->def,
	        prev, prev);
}

hot static void
tree_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_DELETE, arg->id_table, arg->id_storage);

	TreeRow* ref = row_reserved(key);
	rbtree_init_node(&ref->node);

	// delete by key
	Row* prev = NULL;
	RbtreeNode* node;
	int rc;
	rc = tree_find(&self->tree, &self->index.config->def, key, &node);
	if (rc == 0 && node)
	{
		// replace
		prev = tree_row_of(node);
		TreeRow* ref_prev = row_reserved(prev);
		rbtree_remove(&self->tree, &ref_prev->node);
	} else
	{
		// not exists
		return;
	}

	// update transaction log
	log_row(&trx->log, LOG_DELETE,
	        tree_commit,
	        tree_delete_abort,
	        self,
	        self->index.config->primary,
	        arg->id_table,
	        arg->id_storage,
	        &self->index.config->def,
	        key, prev);
}

hot static bool
tree_upsert(Index* arg, Transaction* trx, Iterator** it, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_REPLACE, arg->id_table, arg->id_storage);

	TreeRow* ref = row_reserved(row);
	rbtree_init_node(&ref->node);

	// find existing row
	RbtreeNode* node;
	int rc;
	rc = tree_find(&self->tree, &self->index.config->def, row, &node);
	if (rc == 0 && node)
	{
		*it = tree_iterator_allocate(self);
		auto tree_it = tree_iterator_of(*it);
		tree_it->current  = tree_row_of(node);
		tree_it->pos      = node;
		tree_it->pos_next = NULL;
		return false;
	}

	// insert
	rbtree_set(&self->tree, node, rc, &ref->node);
	self->tree_count++;

	// update transaction log
	log_row(&trx->log, LOG_REPLACE,
	        tree_commit,
	        tree_set_abort,
	        self,
	        self->index.config->primary,
	        arg->id_table,
	        arg->id_storage,
	        &self->index.config->def,
	        row, NULL);
	return true;
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

Index*
tree_allocate(IndexConfig* config, Uuid* table, Uuid* storage)
{
	Tree* self = mn_malloc(sizeof(*self));
	self->tree_count = 0;
	self->lsn        = 0;
	rbtree_init(&self->tree);

	index_init(&self->index, table, storage);
	self->index.free      = tree_free;
	self->index.set       = tree_set;
	self->index.update    = tree_update;
	self->index.delete    = tree_delete;
	self->index.delete_by = tree_delete_by;
	self->index.upsert    = tree_upsert;
	self->index.open      = tree_open;
	self->index.config    = config;
	return &self->index;
}
