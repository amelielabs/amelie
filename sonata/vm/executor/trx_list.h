#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct TrxList TrxList;

struct TrxList
{
	int  list_count;
	List list;
};

static inline void
trx_list_init(TrxList* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
trx_list_reset(TrxList* self, TrxCache* cache)
{
	list_foreach_safe(&self->list)
	{
		auto trx = list_at(Trx, link);
		trx_cache_push(cache, trx);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline Trx*
trx_list_get(TrxList* self)
{
	if (self->list_count == 0)
		return NULL;
	auto first = list_pop(&self->list);
	auto trx = container_of(first, Trx, link);
	self->list_count--;
	return trx;
}

static inline void
trx_list_add(TrxList* self, Trx* trx)
{
	list_append(&self->list, &trx->link);
	self->list_count++;
}

static inline void
trx_list_commit(TrxList* self, Trx* last)
{
	// commit till the last transaction
	list_foreach_safe(&self->list)
	{
		auto trx = list_at(Trx, link);
		transaction_commit(&trx->trx);

		list_unlink(&trx->link);
		self->list_count--;

		trx_cache_push(trx->cache, trx);
		if (trx == last)
			break;
	}
}

static inline void
trx_list_abort(TrxList* self)
{
	// abort all transactions in the list in reverse
	list_foreach_reverse_safe(&self->list)
	{
		auto trx = list_at(Trx, link);
		transaction_abort(&trx->trx);
		trx_cache_push(trx->cache, trx);
	}
	self->list_count = 0;
	list_init(&self->list);
}
