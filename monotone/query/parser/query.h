#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Query Query;

struct Query
{
	bool        in_transaction;
	bool        complete;
	AstStack    stack;
	Cardinality cardinality;
	AstList     stmts;
};

static inline void
query_init(Query* self)
{
	self->in_transaction = false;
	self->complete = false;
	ast_stack_init(&self->stack);
	cardinality_init(&self->cardinality);
	ast_list_init(&self->stmts);
}


static inline void
query_free(Query* self)
{
	cardinality_free(&self->cardinality);
}

static inline void
query_reset(Query* self)
{
	self->in_transaction = false;
	self->complete = false;
	cardinality_reset(&self->cardinality);

	// todo: free
	ast_list_init(&self->stmts);
}

static inline void
query_validate(Query* self, bool transactional)
{
	if (unlikely(self->complete))
		error("transaction is complete");

	if (! transactional)
		if (unlikely(self->in_transaction || self->stmts.count > 0))
			error("operation is not transactional");
}
