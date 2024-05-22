#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct SnapshotId SnapshotId;

struct SnapshotId
{
	uint64_t storage;
	uint64_t lsn;
	bool     incomplete;
};

static inline void
snapshot_id_init(SnapshotId* self)
{
	self->storage    = 0;
	self->lsn        = 0;
	self->incomplete = false;
}

static inline void
snapshot_id_set(SnapshotId* self, uint64_t storage, uint64_t lsn)
{
	self->storage = storage;
	self->lsn     = lsn;
}

static inline void
snapshot_id_path(SnapshotId* self, char* path, bool incomplete)
{
	// <base>/<storage_id>.<lsn>[.incomplete]
	snprintf(path, PATH_MAX,
	         "%s/%" PRIu64 ".%020" PRIu64 "%s",
	         config_directory(),
	         self->storage,
	         self->lsn,
	         incomplete ? ".incomplete" : "");
}
