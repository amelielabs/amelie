#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct CompactMgr CompactMgr;

struct CompactMgr
{
	int      rr;
	int      workers_count;
	Compact* workers;
};

static inline void
compact_mgr_init(CompactMgr* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
compact_mgr_start(CompactMgr* self, int count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = mn_malloc(sizeof(Compact) * count);
	int i = 0;
	for (; i < count; i++)
		compact_init(&self->workers[i]);
	for (i = 0; i < count; i++)
		compact_start(&self->workers[i]);
}

static inline void
compact_mgr_stop(CompactMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		compact_stop(&self->workers[i]);
		compact_free(&self->workers[i]);
	}
	mn_free(self->workers);
	self->workers = NULL;
}

static inline int
compact_mgr_next(CompactMgr* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
compact_mgr_run(CompactMgr* self, CompactReq* req)
{
	assert(self->workers_count > 0);
	int pos = compact_mgr_next(self);
	compact_run(&self->workers[pos], req);
}
