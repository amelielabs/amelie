#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct SnapshotMgr SnapshotMgr;

struct SnapshotMgr
{
	Uuid  uuid;
	IdMgr list_snapshot;
	IdMgr list;
};

void snapshot_mgr_init(SnapshotMgr*);
void snapshot_mgr_free(SnapshotMgr*);
void snapshot_mgr_open(SnapshotMgr*, Uuid*);
void snapshot_mgr_gc(SnapshotMgr*);
void snapshot_mgr_add(SnapshotMgr*, uint64_t);

static inline uint64_t
snapshot_mgr_last(SnapshotMgr* self)
{
	return id_mgr_max(&self->list);
}
