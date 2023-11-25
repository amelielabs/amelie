
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

extern LogOpIf tree_iface;

hot static bool
tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_REPLACE, self->storage);

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
	log_add(&trx->log, LOG_REPLACE, &tree_iface, self,
	        self->index.config->primary,
	        self->storage,
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
	log_reserve(&trx->log, LOG_REPLACE, self->storage);

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
	log_add(&trx->log, LOG_REPLACE, &tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->def,
	        row, prev);
}

hot static void
tree_delete(Index* arg, Transaction* trx, Iterator* it)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_DELETE, self->storage);

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
	log_add(&trx->log, LOG_DELETE, &tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->def,
	        prev, prev);
}

hot static void
tree_delete_by(Index* arg, Transaction* trx, Row* key)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_DELETE, self->storage);

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
	log_add(&trx->log, LOG_DELETE, &tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->def,
	        key, prev);
}

hot static bool
tree_upsert(Index* arg, Transaction* trx, Iterator* it, Row* row)
{
	auto self = tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_REPLACE, self->storage);

	TreeRow* ref = row_reserved(row);
	rbtree_init_node(&ref->node);

	// find existing row
	RbtreeNode* node;
	int rc;
	rc = tree_find(&self->tree, &self->index.config->def, row, &node);
	if (rc == 0 && node)
	{
		// if row exist, set iterator to its position
		auto tree_it = tree_iterator_of(it);
		tree_it->current  = tree_row_of(node);
		tree_it->pos      = node;
		tree_it->pos_next = NULL;
		return false;
	}

	// insert
	rbtree_set(&self->tree, node, rc, &ref->node);
	self->tree_count++;

	// update transaction log
	log_add(&trx->log, LOG_REPLACE, &tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->def,
	        row, NULL);
	return true;
}

hot static Iterator*
tree_read(Index* arg)
{
	auto self = tree_of(arg);
	return tree_iterator_allocate(self);
}

Index*
tree_allocate(IndexConfig* config, Uuid* storage)
{
	Tree* self = mn_malloc(sizeof(*self));
	self->tree_count = 0;
	self->lsn        = 0;
	self->storage    = storage;
	rbtree_init(&self->tree);

	index_init(&self->index);
	self->index.free      = tree_free;
	self->index.set       = tree_set;
	self->index.update    = tree_update;
	self->index.delete    = tree_delete;
	self->index.delete_by = tree_delete_by;
	self->index.upsert    = tree_upsert;
	self->index.read      = tree_read;

	guard(guard, tree_free, self);
	self->index.config = index_config_copy(config);
	def_set_reserved(&self->index.config->def, sizeof(TreeRow));
	unguard(&guard);
	return &self->index;
}

static void
tree_abort(void* arg, LogCmd cmd, Row* row, Row* prev)
{
	Tree* self = arg;
	RbtreeNode* node;
	int rc;
	if (cmd == LOG_REPLACE)
	{
		TreeRow* ref = row_reserved(row);
		if (prev)
		{
			// abort replace
			TreeRow* ref_prev = row_reserved(prev);
			rbtree_replace(&self->tree, &ref->node, &ref_prev->node);
		} else
		{
			// abort insert
			rbtree_remove(&self->tree, &ref->node);
			self->tree_count--;
		}
		row_free(row);
	} else
	{
		// abort delete
		rc = tree_find(&self->tree, &self->index.config->def, prev, &node);
		TreeRow* ref_prev = row_reserved(prev);
		rbtree_init_node(&ref_prev->node);
		rbtree_set(&self->tree, node, rc, &ref_prev->node);
		self->tree_count++;
		// delete row is not allocated
	}
}

LogOpIf tree_iface =
{
	.abort = tree_abort
};
