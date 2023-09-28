
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>

typedef struct IndexTreeRow IndexTreeRow;
typedef struct IndexTree    IndexTree;

struct IndexTreeRow
{
	RbtreeNode node;
};

struct IndexTree
{
	Index    index;
	Rbtree   tree;
	int      tree_count;
	Uuid*    storage;
	uint64_t lsn;
};

always_inline static inline Row*
index_tree_row_of(RbtreeNode* self)
{
	return row_reserved_row(self);
}

always_inline static inline IndexTree*
index_tree_of(Index* self)
{
	return (IndexTree*)self;
}

rbtree_free(index_tree_truncate, row_free(index_tree_row_of(n)))

void
index_tree_free(Index* arg)
{
	auto self = index_tree_of(arg);
	if (self->tree.root)
		index_tree_truncate(self->tree.root, NULL);
	rbtree_init(&self->tree);
	self->tree_count = 0;
	mn_free(self);
}

hot static inline
rbtree_get(index_tree_find, compare(arg, index_tree_row_of(n), key));

extern LogOpIf index_tree_iface;

hot static Row*
index_tree_set(Index* arg, Transaction* trx, Row* row)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_REPLACE, self->storage);

	IndexTreeRow* ref = row_reserved(row);
	rbtree_init_node(&ref->node);

	// replace
	Row* prev = NULL;
	RbtreeNode* node;
	int rc;
	rc = index_tree_find(&self->tree, &self->index.config->schema, row, &node);
	if (rc == 0 && node)
	{
		// replace
		prev = index_tree_row_of(node);
		IndexTreeRow* ref_prev = row_reserved(prev);

		rbtree_replace(&self->tree, &ref_prev->node, &ref->node);
	} else
	{
		// insert
		rbtree_set(&self->tree, node, rc, &ref->node);
		self->tree_count++;
	}

	// update transaction log
	log_add(&trx->log, LOG_REPLACE, &index_tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->schema,
	        row, prev);
	return prev;
}

hot static Row*
index_tree_delete(Index* arg, Transaction* trx, Row* key)
{
	auto self = index_tree_of(arg);

	// reserve log
	log_reserve(&trx->log, LOG_DELETE, self->storage);

	IndexTreeRow* ref = row_reserved(key);
	rbtree_init_node(&ref->node);

	// delete by key
	Row* prev = NULL;
	RbtreeNode* node;
	int rc;
	rc = index_tree_find(&self->tree, &self->index.config->schema, key, &node);
	if (rc == 0 && node)
	{
		// replace
		prev = index_tree_row_of(node);
		IndexTreeRow* ref_prev = row_reserved(prev);
		rbtree_remove(&self->tree, &ref_prev->node);
	} else
	{
		// not exists
		return NULL;
	}

	// update transaction log
	log_add(&trx->log, LOG_DELETE, &index_tree_iface, self,
	        self->index.config->primary,
	        self->storage,
	        &self->index.config->schema,
	        key, prev);
	return prev;
}

hot static Row*
index_tree_get(Index* arg, Row* key)
{
	auto self = index_tree_of(arg);
	RbtreeNode* node;
	int rc;
	rc = index_tree_find(&self->tree, &self->index.config->schema, key, &node);
	if (rc == 0 && node)
		return index_tree_row_of(node);
	return NULL;
}

hot static Iterator*
index_tree_read(Index* arg)
{
	auto self = index_tree_of(arg);
	(void)self;
	return NULL;
}

Index*
index_tree_allocate(IndexConfig* config, Uuid* storage)
{
	IndexTree* self = mn_malloc(sizeof(*self));
	self->tree_count = 0;
	self->lsn        = 0;
	self->storage    = storage;
	rbtree_init(&self->tree);

	index_init(&self->index);
	self->index.free   = index_tree_free;
	self->index.set    = index_tree_set;
	self->index.delete = index_tree_delete;
	self->index.get    = index_tree_get;
	self->index.read   = index_tree_read;

	guard(guard, index_tree_free, self);
	self->index.config = index_config_copy(config);
	schema_set_reserved(&self->index.config->schema, sizeof(IndexTreeRow));
	unguard(&guard);
	return &self->index;
}

static void
index_tree_abort(void* arg, LogCmd cmd, Row* row, Row* prev)
{
	IndexTree* self = arg;
	RbtreeNode* node;
	int rc;
	if (cmd == LOG_REPLACE)
	{
		IndexTreeRow* ref = row_reserved(row);
		if (prev)
		{
			// abort replace
			IndexTreeRow* ref_prev = row_reserved(prev);
			rbtree_replace(&self->tree, &ref->node, &ref_prev->node);
		} else
		{
			// abort insert
			rbtree_remove(&self->tree, &ref->node);
			self->tree_count--;
		}
	} else
	{
		// abort delete
		rc = index_tree_find(&self->tree, &self->index.config->schema, prev, &node);
		IndexTreeRow* ref_prev = row_reserved(prev);
		rbtree_init_node(&ref_prev->node);
		rbtree_set(&self->tree, node, rc, &ref_prev->node);
		self->tree_count++;
	}
	row_free(row);
}

LogOpIf index_tree_iface =
{
	.abort = index_tree_abort
};
