#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct SnapshotId SnapshotId;

struct SnapshotId
{
	Uuid     uuid;
	char     uuid_sz[UUID_SZ];
	uint64_t lsn;
	bool     incomplete;
};

static inline void
snapshot_id_init(SnapshotId* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
snapshot_id_set(SnapshotId* self, Uuid* uuid, uint64_t lsn)
{
	self->lsn  =  lsn;
	self->uuid = *uuid;
	uuid_to_string(&self->uuid, self->uuid_sz, sizeof(self->uuid_sz));
}

static inline void
snapshot_id_path(SnapshotId* self, char* path, bool incomplete)
{
	// <base>/storage_uuid/lsn[.incomplete]
	snprintf(path, PATH_MAX,
	         "%s/%s/%020" PRIu64 "%s",
	         config_directory(),
	         self->uuid_sz,
	         self->lsn,
	         incomplete ? ".incomplete" : "");
}

/*
static inline void
snapshot_id_path_storage(SnapshotId* self, char* path)
{
	// <base>/storage_uuid
	snprintf(path, PATH_MAX, "%s/%s", config_directory(), self->uuid_sz);
}
*/

static inline void
snapshot_id_path_name(SnapshotId* self, char* path, bool incomplete)
{
	// lsn[.incomplete]
	snprintf(path, PATH_MAX, "%020" PRIu64 "%s", self->lsn,
	         incomplete ? ".incomplete" : "");
}
