#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PlanGroup PlanGroup;

struct PlanGroup
{
	TrxSet   set;
	int      list_count;
	List     list;
	WalBatch wal_batch;
};

static inline void
plan_group_init(PlanGroup* self)
{
	self->list_count = 0;
	list_init(&self->list);
	trx_set_init(&self->set);
	wal_batch_init(&self->wal_batch);
}

static inline void
plan_group_free(PlanGroup* self)
{
	trx_set_free(&self->set);
	wal_batch_free(&self->wal_batch);
}

static inline void
plan_group_reset(PlanGroup* self)
{
	self->list_count = 0;
	list_init(&self->list);
	trx_set_reset(&self->set);
	wal_batch_reset(&self->wal_batch);
}

static inline void
plan_group_create(PlanGroup* self, int set_size)
{
	trx_set_create(&self->set, set_size);
}

static inline void
plan_group_add(PlanGroup* self, Plan* plan)
{
	// collect a list of last completed transactions per route
	trx_set_resolve(&plan->set, &self->set);

	list_append(&self->list, &plan->link_group);
	self->list_count++;
}
