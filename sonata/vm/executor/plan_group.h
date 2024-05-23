#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct PlanGroup PlanGroup;

struct PlanGroup
{
	int      list_count;
	List     list;
	Trx**    set;
	WalBatch wal_batch;
	Router*  router;
};

static inline void
plan_group_init(PlanGroup* self, Router* router)
{
	self->router     = router;
	self->set        = NULL;
	self->list_count = 0;
	list_init(&self->list);
	wal_batch_init(&self->wal_batch);
}

static inline void
plan_group_free(PlanGroup* self)
{
	if (self->set)
		so_free(self->set);
	wal_batch_free(&self->wal_batch);
}

static inline void
plan_group_reset(PlanGroup* self)
{
	if (self->set)
	{
		int size = sizeof(Trx*) * self->router->set_size;
		memset(self->set, 0, size);
	}
	self->list_count = 0;
	list_init(&self->list);
	wal_batch_reset(&self->wal_batch);
}

static inline void
plan_group_add(PlanGroup* self, Plan* plan)
{
	// collect a list of last completed transactions per shard
	dispatch_resolve(&plan->dispatch, self->set);

	list_append(&self->list, &plan->link_group);
	self->list_count++;
}

static inline void
plan_group_create(PlanGroup* self)
{
	if (self->set)
		return;
	int size = sizeof(Trx*) * self->router->set_size;
	self->set = so_malloc(size);
	memset(self->set, 0, size);
}

static inline void
plan_group_send(PlanGroup* self, int id)
{
	auto router = self->router;
	for (int i = 0; i < router->set_size; i++)
	{
		if (self->set[i] == NULL)
			continue;
		auto route = router_at(router, i);
		auto buf = msg_begin(id);
		buf_write(buf, &self->set[i], sizeof(void**));
		msg_end(buf);
		channel_write(route->channel, buf);
	}
}

static inline void
plan_group_commit(PlanGroup* self)
{
	plan_group_send(self, RPC_COMMIT);
}

static inline void
plan_group_abort(PlanGroup* self)
{
	plan_group_send(self, RPC_ABORT);
}
