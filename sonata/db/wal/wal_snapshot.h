#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct WalSnapshot WalSnapshot;

struct WalSnapshot
{
	uint64_t snapshot;
	Buf      list;
	int      list_count;
	uint64_t lsn;
	uint64_t last_id;
	uint64_t last_size;
	IdMgr*   id_mgr;
};

static inline void
wal_snapshot_init(WalSnapshot* self)
{
	self->snapshot   = UINT64_MAX;
	self->lsn        = 0;
	self->last_id    = 0;
	self->last_size  = 0;
	self->list_count = 0;
	self->id_mgr     = NULL;
	buf_init(&self->list);
}

static inline void
wal_snapshot_free(WalSnapshot* self)
{
	if (self->snapshot != UINT64_MAX)
	{
		id_mgr_delete(self->id_mgr, self->snapshot);
		self->snapshot = UINT64_MAX;
	}
	buf_free(&self->list);
}

static inline uint64_t
wal_snapshot_at(WalSnapshot* self, int order, uint64_t* size)
{
	if (order >= self->list_count)
		return UINT64_MAX;
	uint64_t id;
	id = ((uint64_t*)self->list.start)[order];
	if (id == self->last_id)
		*size = self->last_size;
	else
		*size = UINT64_MAX;
	return id;
}
